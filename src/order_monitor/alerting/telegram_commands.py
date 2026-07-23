"""텔레그램 수신 명령 루프 (PRD §9.5 v1.14) — W 주시 레벨의 런타임 조작.

주시 등록/해소는 재시작 없이 이루어져야 한다 — 재시작은 epoch 종료(활성
인텐트 INTERRUPTED, trade_window·D4 스트릭 리셋)를 수반하므로. 발송 전용이던
봇에 `getUpdates` 롱폴 수신을 추가한다:

- **격리**: 발송기와 동일한 부분 실패 격리 (§11.1) — 수신 장애·API 오류가
  파이프라인을 막지 않고, 지수 백오프(1→60s)로 재시도한다. 409 Conflict는
  단일 소비자 위반(로컬+VPS 동시 실행) 안내 로그를 남긴다.
- **수신 대상 (v1.14)**: `message` + `channel_post` — 채널 게시물은 Bot API에서
  별도 업데이트 타입이라 `message`만 읽으면 채널 명령이 무기록 스킵된다
  (실배포 검증 발견, PRD v1.14 개정 이력).
- **인증 (v1.14 복수화)**: 발신 `chat.id` ∈ `telegram.command_chat_ids` 목록.
  불일치는 무시(구조화 로그만) — 봇은 공개 발견 가능하므로 이것이 접근 통제다
  (채널은 게시 권한자만 글 작성 가능 — 이중 통제). 발송 대상(`telegram.chat_id`)
  과 분리되며, 명령 응답은 발신 chat으로 라우팅한다.
- **offset 영속**: 처리한 `update_id`를 KVStore(§12.2)에 즉시 저장 — 미저장
  시 재시작에 이전 명령이 재실행된다 (멱등이라 피해는 제한적, 응답 중복 방지).
- **문법**: `/watch <price>` · `/watch <lo>-<hi>` · `/unwatch <동일 문법>` ·
  `/watching`. 성공/실패를 즉시 응답, 그 외 텍스트는 무시. 그룹 챗의
  `/watch@BotName` 접미도 수용한다.

명령의 실제 처리(관측기/store 갱신)는 service가 주입한 동기 콜백 소관 —
수신 루프는 파싱·인증·응답 릴레이만 담당한다.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from decimal import Decimal, InvalidOperation

import aiohttp

logger = logging.getLogger(__name__)

OFFSET_KV_KEY = "telegram_update_offset"
LONG_POLL_SECONDS = 50
REQUEST_TIMEOUT_SECONDS = 60.0  # 롱폴보다 길게
INITIAL_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 60.0

USAGE_TEXT = "형식: /watch 65600 · /watch 64900-66000 · /unwatch <동일 문법> · /watching"


def parse_zone(arg: str) -> tuple[Decimal, Decimal] | None:
    """가격 인자 파싱 — 단일가(폭 0 구역) 또는 lo-hi 범위. 실패 시 None."""
    arg = arg.strip()
    if not arg:
        return None
    parts = arg.split("-")
    try:
        if len(parts) == 1:
            price = Decimal(parts[0])
            zone = (price, price)
        elif len(parts) == 2:
            a, b = Decimal(parts[0]), Decimal(parts[1])
            zone = (a, b) if a <= b else (b, a)
        else:
            return None
    except InvalidOperation:
        return None
    if zone[0] <= 0:
        return None
    return zone


class TelegramReceiver:
    def __init__(
        self,
        token: str,
        command_chat_ids: list[str],
        *,
        on_watch: Callable[[Decimal, Decimal, str], str],
        on_unwatch: Callable[[Decimal, Decimal, str], str],
        on_watching: Callable[[], str],
        send: Callable[[str, str], None],  # 응답 발송 (text, 발신 chat_id) — v1.14 라우팅
        kv_get: Callable[[str], str | None],
        kv_set: Callable[[str, str], None],
        base_url: str = "https://api.telegram.org",
        sleep=asyncio.sleep,
        get=None,  # 테스트 주입점: (url, params) → (status, body)
    ) -> None:
        self._token = token
        self._url = f"{base_url}/bot{token}/getUpdates"
        self._allowed_chat_ids = frozenset(command_chat_ids)
        self._on_watch = on_watch
        self._on_unwatch = on_unwatch
        self._on_watching = on_watching
        self._send = send
        self._kv_get = kv_get
        self._kv_set = kv_set
        self._sleep = sleep
        self._get = get or self._default_get

    async def run(self) -> None:
        """수신 워커 — 취소로만 종료. 실패는 백오프 재시도, 파이프라인 불침범."""
        backoff = INITIAL_BACKOFF_SECONDS
        while True:
            try:
                ok = await self.poll_once()
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError, json.JSONDecodeError) as exc:
                logger.warning(
                    "telegram getUpdates error",
                    extra={"error": f"{type(exc).__name__}: {self._redact(str(exc))}"},
                )
                ok = False
            if ok:
                backoff = INITIAL_BACKOFF_SECONDS
            else:
                await self._sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)

    async def poll_once(self) -> bool:
        """getUpdates 1사이클. 정상 처리 시 True, 오류 응답 시 False."""
        offset = self._kv_get(OFFSET_KV_KEY)
        params: dict = {"timeout": LONG_POLL_SECONDS}
        if offset is not None:
            params["offset"] = int(offset) + 1
        status, body = await self._get(self._url, params)
        if status == 409:
            # 단일 소비자 제약 위반 — 로컬+VPS 동시 실행 (PRD §9.5 운영 주의)
            logger.warning("telegram getUpdates 409 conflict — another consumer is polling")
            return False
        if status != 200:
            logger.warning(
                "telegram getUpdates failed",
                extra={"status": status, "body": self._redact(body)[:200]},
            )
            return False
        payload = json.loads(body)
        for update in payload.get("result", []):
            self._process_update(update)
            self._kv_set(OFFSET_KV_KEY, str(update["update_id"]))
        return True

    # ---- 내부 -----------------------------------------------------------

    def _process_update(self, update: dict) -> None:
        # 채널 게시는 message가 아닌 channel_post로 도착 (v1.14 — §9.5)
        message = update.get("message") or update.get("channel_post") or {}
        chat_id = str((message.get("chat") or {}).get("id"))
        text = message.get("text")
        if not isinstance(text, str):
            return
        if chat_id not in self._allowed_chat_ids:
            logger.warning(
                "telegram command from unauthorized chat ignored",
                extra={"chat_id": chat_id},
            )
            return
        response = self._handle_text(text)
        if response is not None:
            self._send(response, chat_id)  # 응답은 발신 chat으로 (v1.14)

    def _handle_text(self, text: str) -> str | None:
        parts = text.strip().split(maxsplit=1)
        if not parts:
            return None
        command = parts[0].split("@", 1)[0]  # 그룹 챗 "/watch@BotName" 수용
        arg = parts[1].strip() if len(parts) > 1 else ""

        if command == "/watching":
            return self._on_watching()
        if command not in ("/watch", "/unwatch"):
            return None  # 명령 외 텍스트는 무시 (§9.5)
        zone = parse_zone(arg)
        if zone is None:
            return f"인식할 수 없는 가격입니다: {arg!r}\n{USAGE_TEXT}"
        lo, hi = zone
        if command == "/watch":
            return self._on_watch(lo, hi, arg)
        return self._on_unwatch(lo, hi, arg)

    def _redact(self, text: str) -> str:
        return text.replace(self._token, "***") if self._token else text

    async def _default_get(self, url: str, params: dict) -> tuple[int, str]:
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params) as resp:
                return resp.status, await resp.text()
