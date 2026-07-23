"""TelegramReceiver 단위 테스트 (PRD §9.5 v1.13)."""

import asyncio
import json
from decimal import Decimal

import pytest

from order_monitor.alerting.telegram_commands import (
    OFFSET_KV_KEY,
    TelegramReceiver,
    parse_zone,
)

CHAT_ID = "-1001234"
DM_CHAT_ID = "777001"


class FakeKV:
    def __init__(self):
        self.data: dict[str, str] = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value


class Harness:
    def __init__(self, responses=None, allowed=None):
        self.kv = FakeKV()
        self.sent: list[str] = []
        self.sent_chats: list[str] = []
        self.watch_calls: list[tuple] = []
        self.unwatch_calls: list[tuple] = []
        self.watching_calls = 0
        self.get_calls: list[dict] = []
        self._responses = list(responses or [])
        self._allowed = allowed if allowed is not None else [CHAT_ID]

    async def get(self, url, params):
        self.get_calls.append(params)
        if not self._responses:
            return 200, json.dumps({"ok": True, "result": []})
        return self._responses.pop(0)

    def _send(self, text, chat_id):
        self.sent.append(text)
        self.sent_chats.append(chat_id)

    def receiver(self) -> TelegramReceiver:
        return TelegramReceiver(
            "test-token",
            self._allowed,
            on_watch=lambda lo, hi, arg: self.watch_calls.append((lo, hi, arg)) or f"주시 등록: {arg}",
            on_unwatch=lambda lo, hi, arg: self.unwatch_calls.append((lo, hi, arg)) or f"주시 해소: {arg}",
            on_watching=self._watching,
            send=self._send,
            kv_get=self.kv.get,
            kv_set=self.kv.set,
            get=self.get,
        )

    def _watching(self):
        self.watching_calls += 1
        return "현황"


def updates_body(*messages, start_id=100):
    result = [
        {"update_id": start_id + i, "message": msg} for i, msg in enumerate(messages)
    ]
    return json.dumps({"ok": True, "result": result})


def msg(text, chat_id=CHAT_ID):
    return {"chat": {"id": int(chat_id)}, "text": text}


def channel_body(*posts, start_id=100):
    # 채널 게시는 message가 아닌 channel_post로 도착 (v1.14 — PRD §9.5)
    result = [
        {"update_id": start_id + i, "channel_post": post} for i, post in enumerate(posts)
    ]
    return json.dumps({"ok": True, "result": result})


def run(coro):
    return asyncio.run(coro)


class TestParseZone:
    def test_single_price(self):
        assert parse_zone("65600") == (Decimal(65600), Decimal(65600))

    def test_range(self):
        assert parse_zone("64900-66000") == (Decimal(64900), Decimal(66000))

    def test_reversed_range_swapped(self):
        assert parse_zone("66000-64900") == (Decimal(64900), Decimal(66000))

    def test_decimal_price(self):
        assert parse_zone("65600.5") == (Decimal("65600.5"), Decimal("65600.5"))

    @pytest.mark.parametrize("bad", ["abc", "", "65000-", "-65000", "1-2-3", "0"])
    def test_invalid_inputs(self, bad):
        assert parse_zone(bad) is None


class TestCommands:
    def test_watch_command_dispatches_and_responds(self):
        h = Harness([(200, updates_body(msg("/watch 65600")))])
        run(h.receiver().poll_once())
        assert h.watch_calls == [(Decimal(65600), Decimal(65600), "65600")]
        assert h.sent == ["주시 등록: 65600"]

    def test_range_watch(self):
        h = Harness([(200, updates_body(msg("/watch 64900-66000")))])
        run(h.receiver().poll_once())
        assert h.watch_calls == [(Decimal(64900), Decimal(66000), "64900-66000")]

    def test_unwatch_command(self):
        h = Harness([(200, updates_body(msg("/unwatch 65600")))])
        run(h.receiver().poll_once())
        assert h.unwatch_calls == [(Decimal(65600), Decimal(65600), "65600")]

    def test_watching_command(self):
        h = Harness([(200, updates_body(msg("/watching")))])
        run(h.receiver().poll_once())
        assert h.watching_calls == 1
        assert h.sent == ["현황"]

    def test_group_bot_suffix_accepted(self):
        h = Harness([(200, updates_body(msg("/watching@MyBot")))])
        run(h.receiver().poll_once())
        assert h.watching_calls == 1

    def test_bad_price_gets_error_response(self):
        h = Harness([(200, updates_body(msg("/watch abc")))])
        run(h.receiver().poll_once())
        assert h.watch_calls == []
        assert "인식할 수 없는 가격" in h.sent[0]

    def test_non_command_text_ignored(self):
        h = Harness([(200, updates_body(msg("안녕"), msg("/start")))])
        run(h.receiver().poll_once())
        assert h.sent == []

    def test_unauthorized_chat_ignored(self):
        h = Harness([(200, updates_body(msg("/watch 65600", chat_id="777")))])
        run(h.receiver().poll_once())
        assert h.watch_calls == []
        assert h.sent == []
        # offset은 여전히 전진 — 무권한 메시지를 재처리하지 않는다
        assert h.kv.data[OFFSET_KV_KEY] == "100"


class TestOffset:
    def test_offset_persisted_per_update(self):
        h = Harness([(200, updates_body(msg("/watching"), msg("/watching")))])
        run(h.receiver().poll_once())
        assert h.kv.data[OFFSET_KV_KEY] == "101"

    def test_next_poll_uses_offset_plus_one(self):
        h = Harness(
            [
                (200, updates_body(msg("/watching"))),
                (200, json.dumps({"ok": True, "result": []})),
            ]
        )
        receiver = h.receiver()
        run(receiver.poll_once())
        run(receiver.poll_once())
        assert h.get_calls[0].get("offset") is None  # 최초 — offset 없음
        assert h.get_calls[1]["offset"] == 101

    def test_long_poll_timeout_param(self):
        h = Harness()
        run(h.receiver().poll_once())
        assert h.get_calls[0]["timeout"] == 50


class TestFailureIsolation:
    def test_409_conflict_returns_false(self):
        h = Harness([(409, "conflict")])
        assert run(h.receiver().poll_once()) is False
        assert h.sent == []

    def test_http_error_returns_false(self):
        h = Harness([(500, "boom")])
        assert run(h.receiver().poll_once()) is False

    def test_run_backs_off_on_errors_and_recovers(self):
        # run() 루프: 오류 → 백오프 sleep → 성공 시 백오프 리셋
        h = Harness(
            [
                (500, "boom"),
                (500, "boom"),
                (200, updates_body(msg("/watching"))),
            ]
        )
        sleeps: list[float] = []
        receiver = h.receiver()
        stop = asyncio.CancelledError

        async def fake_sleep(seconds):
            sleeps.append(seconds)

        receiver._sleep = fake_sleep

        async def drive():
            # 응답 소진 후의 4번째 poll에서 취소해 루프를 끝낸다
            original_get = h.get

            async def get_then_stop(url, params):
                if not h._responses:
                    raise stop()
                return await original_get(url, params)

            receiver._get = get_then_stop
            with pytest.raises(stop):
                await receiver.run()

        run(drive())
        assert sleeps == [1.0, 2.0]  # 지수 백오프, 성공 후 추가 sleep 없음
        assert h.watching_calls == 1


class TestChannelAndMultiChat:
    def test_channel_post_command_processed(self):
        # v1.14 — 채널 게시(channel_post)도 명령으로 수신 (실배포 발견)
        h = Harness([(200, channel_body(msg("/watching")))])
        run(h.receiver().poll_once())
        assert h.watching_calls == 1
        assert h.sent == ["현황"]
        assert h.sent_chats == [CHAT_ID]

    def test_response_routed_to_originating_chat(self):
        # 허용 목록의 DM에서 온 명령 — 응답은 알림 채널이 아닌 발신 chat으로
        h = Harness(
            [(200, updates_body(msg("/watch 65600", chat_id=DM_CHAT_ID)))],
            allowed=[CHAT_ID, DM_CHAT_ID],
        )
        run(h.receiver().poll_once())
        assert h.watch_calls == [(Decimal(65600), Decimal(65600), "65600")]
        assert h.sent_chats == [DM_CHAT_ID]

    def test_multiple_allowed_chats_both_work(self):
        h = Harness(
            [
                (
                    200,
                    updates_body(
                        msg("/watching"), msg("/watching", chat_id=DM_CHAT_ID)
                    ),
                )
            ],
            allowed=[CHAT_ID, DM_CHAT_ID],
        )
        run(h.receiver().poll_once())
        assert h.watching_calls == 2
        assert h.sent_chats == [CHAT_ID, DM_CHAT_ID]

    def test_chat_not_in_list_still_ignored(self):
        h = Harness(
            [(200, updates_body(msg("/watch 65600", chat_id="999")))],
            allowed=[CHAT_ID, DM_CHAT_ID],
        )
        run(h.receiver().poll_once())
        assert h.watch_calls == []
        assert h.sent == []

    def test_channel_post_from_unauthorized_channel_ignored(self):
        h = Harness([(200, channel_body(msg("/watching", chat_id="-100999")))])
        run(h.receiver().poll_once())
        assert h.watching_calls == 0
        assert h.sent == []
