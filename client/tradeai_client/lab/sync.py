"""Синхронизация пространства профиля с сервером (кнопка «Синхронизировать с сервером»): уроки, которых нет на сервере, уходят туда; уроки этого же
профиля с других ПК скачиваются; свои понятия словаря объединяются. Только ОДНО пространство (santa | chr) - уроки другого профиля не трогаются."""

from typing import Any

from tradeai_client.lab import remote, store


def sync(base: str, key: str, profile: str) -> dict[str, Any]:
    space, root = store.space_of(profile), store.lab_dir(profile)
    local = {x["id"] for x in store.list_lessons(root)}
    remote_ids = {x["id"] for x in remote.list_lessons(base, key, space)}
    up = down = 0
    for lid in sorted(local - remote_ids):
        remote.push_lesson(base, key, space, store.load_lesson(lid, root))
        up += 1
    for lid in sorted(remote_ids - local):
        try:
            store.save_lesson(remote.get_lesson(base, key, space, lid), root)
            down += 1
        except store.StoreError:
            continue  # битый урок на сервере не мешает остальным
    snap_local = {x["id"] for x in store.list_snapshots(root)}
    snap_remote = {x["id"] for x in remote.list_snapshots(base, key, space)}
    snap_up = snap_down = 0
    for sid in sorted(snap_local - snap_remote):
        remote.push_snapshot(base, key, space, store.load_snapshot(sid, root, with_image=True))
        snap_up += 1
    for sid in sorted(snap_remote - snap_local):
        try:
            store.save_snapshot(remote.get_snapshot(base, key, space, sid), root)
            snap_down += 1
        except store.StoreError:
            continue
    mine = [t.to_dict() for t in store.load_vocab(root) if not t.builtin]
    known = {t["id"] for t in mine}
    merged = mine + [t for t in remote.get_vocab(base, key, space) if t.get("id") not in known]
    if merged:
        store._write(root / "vocab.json", merged)
        remote.push_vocab(base, key, space, merged)
    return {"space": space, "up": up, "down": down, "terms": len(merged), "snap_up": snap_up, "snap_down": snap_down}
