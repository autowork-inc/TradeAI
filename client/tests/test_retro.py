from tradeai_client.retro import review_trade


def trade(r, **kw):
    t = {"status": "closed", "side": "long", "r_multiple": r, "mfe_r": None, "mae_r": None, "entry_price": 100.0, "sl": 98.0, "tp": 106.0, "exit_reason": "sl"}
    t.update(kw)
    return t


def bars(high=100.0, low=100.0):
    return [{"h": high, "l": low}]


def texts(rv):
    return " | ".join(p["text"] for p in rv["points"])


def test_open_trade_has_no_review():
    assert review_trade(trade(1.0, status="open"), []) is None and review_trade(trade(None), []) is None


def test_ideal_scale_out_scores_high():
    closes = [{"reason": "scale_1r"}, {"reason": "scale_3r"}, {"reason": "trail"}]
    rv = review_trade(trade(2.14, mfe_r=2.3, mae_r=-0.1, exit_reason="trail"), closes)
    assert rv["score"] >= 9 and "Идеальная отработка Scale Out" in texts(rv) and rv["headline"].startswith(f"{rv['score']}/10")


def test_stop_too_tight_when_price_reaches_target_afterwards():
    rv = review_trade(trade(-1.0, mfe_r=0.2, mae_r=1.0), [{"reason": "sl"}], bars(high=107.0, low=97.0))
    assert "Слишком короткий стоп" in texts(rv) and rv["score"] <= 3
    assert "Слишком короткий стоп" not in texts(review_trade(trade(-1.0, mfe_r=0.2, mae_r=1.0), [{"reason": "sl"}], bars(high=101.0)))
    assert "Слишком короткий стоп" not in texts(review_trade(trade(-1.0), [{"reason": "sl"}], None))  # свечей после выхода нет - не утверждаем


def test_short_side_target_after_exit():
    t = trade(-1.0, side="short", entry_price=100.0, sl=102.0, tp=94.0)
    assert "Слишком короткий стоп" in texts(review_trade(t, [{"reason": "sl"}], bars(high=103.0, low=93.0)))


def test_early_entry_and_giveback_penalized():
    rv = review_trade(trade(0.3, mfe_r=2.5, mae_r=0.9, exit_reason="be"), [{"reason": "scale_1r"}, {"reason": "be"}])
    assert "Ранний вход" in texts(rv) and "Отдана большая часть хода" in texts(rv)


def test_loser_that_was_green_and_breakeven_and_timeout():
    assert "защита прибыли не сработала" in texts(review_trade(trade(-1.0, mfe_r=1.4), [{"reason": "sl"}]))
    assert review_trade(trade(0.0, exit_reason="be"), [{"reason": "be"}])["points"][0]["tone"] == "neutral"
    assert "Таймаут" in texts(review_trade(trade(-0.2, exit_reason="timeout"), [{"reason": "timeout"}]))


def test_score_is_clamped():
    assert 1 <= review_trade(trade(-1.0, mfe_r=0.0, mae_r=2.0), [{"reason": "sl"}], bars(high=110))["score"] <= 10
    assert review_trade(trade(5.0, mfe_r=5.0, mae_r=0.0, exit_reason="tp"), [{"reason": "scale_1r"}, {"reason": "scale_3r"}, {"reason": "tp"}])["score"] == 10
