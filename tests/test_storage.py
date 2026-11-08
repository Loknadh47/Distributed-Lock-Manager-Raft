import os

import pytest

from dlm.raft import CorruptWAL, FileStorage


def test_roundtrip_hard_state_and_log(tmp_path):
    s = FileStorage(str(tmp_path), fsync=False)
    s.save_hard_state(3, "n2")
    s.write_entries(1, [(1, {"a": 1}), (1, None), (2, {"b": 2})])
    s.close()
    term, voted, entries = FileStorage(str(tmp_path), fsync=False).load()
    assert (term, voted) == (3, "n2")
    assert entries == [(1, {"a": 1}), (1, None), (2, {"b": 2})]


def test_write_truncates_conflicting_suffix(tmp_path):
    s = FileStorage(str(tmp_path), fsync=False)
    s.write_entries(1, [(1, "a"), (1, "b"), (1, "c")])
    s.write_entries(2, [(2, "B")])  # new leader overwrites index 2.. and drops 3
    s.close()
    _, _, entries = FileStorage(str(tmp_path), fsync=False).load()
    assert entries == [(1, "a"), (2, "B")]


def test_torn_tail_is_discarded_and_file_stays_appendable(tmp_path):
    s = FileStorage(str(tmp_path), fsync=False)
    s.write_entries(1, [(1, "a")])
    s.close()
    wal = os.path.join(tmp_path, "raft.wal")
    with open(wal, "ab") as fh:
        fh.write(b'{"k":"w","i":2,"e":[[1,"half-writ')  # crash mid-write, no newline
    s2 = FileStorage(str(tmp_path), fsync=False)
    assert s2.load()[2] == [(1, "a")]
    s2.write_entries(2, [(1, "b")])
    s2.close()
    assert FileStorage(str(tmp_path), fsync=False).load()[2] == [(1, "a"), (1, "b")]


def test_corruption_in_the_middle_is_an_error(tmp_path):
    wal = os.path.join(tmp_path, "raft.wal")
    with open(wal, "wb") as fh:
        fh.write(b'{"k":"hs","t":1,"v":null}\nNOT JSON\n{"k":"hs","t":2,"v":null}\n')
    with pytest.raises(CorruptWAL):
        FileStorage(str(tmp_path), fsync=False)
