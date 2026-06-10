from datetime import datetime

import app as dashboard


def write_metadata(data_dir, timestamp):
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / 'metadata.csv').write_text(
        f'last_updated,files\n{timestamp},"[]"\n',
        encoding='utf-8',
    )


def test_sync_bundled_data_copies_newer_csvs(tmp_path, monkeypatch):
    bundled = tmp_path / 'repo_data'
    external = tmp_path / 'disk_data'
    write_metadata(bundled, '2026-06-01T12:00:00')
    write_metadata(external, '2026-05-01T12:00:00')
    (bundled / 'sample.csv').write_text('date,value\n2026-06-01,2\n', encoding='utf-8')
    (external / 'sample.csv').write_text('date,value\n2026-05-01,1\n', encoding='utf-8')

    monkeypatch.setattr(dashboard, 'BUNDLED_DATA_DIR', bundled)
    monkeypatch.setattr(dashboard, 'DATA_DIR', external)
    monkeypatch.setenv('SYNC_BUNDLED_DATA_ON_STARTUP', '1')

    state = dashboard.sync_bundled_data_if_newer()

    assert state['status'] == 'completed'
    assert sorted(state['files_copied']) == ['metadata.csv', 'sample.csv']
    assert (external / 'sample.csv').read_text(encoding='utf-8') == 'date,value\n2026-06-01,2\n'


def test_sync_bundled_data_keeps_newer_external_data(tmp_path, monkeypatch):
    bundled = tmp_path / 'repo_data'
    external = tmp_path / 'disk_data'
    write_metadata(bundled, '2026-05-01T12:00:00')
    write_metadata(external, '2026-06-01T12:00:00')
    (bundled / 'sample.csv').write_text('date,value\n2026-05-01,1\n', encoding='utf-8')
    (external / 'sample.csv').write_text('date,value\n2026-06-01,2\n', encoding='utf-8')

    monkeypatch.setattr(dashboard, 'BUNDLED_DATA_DIR', bundled)
    monkeypatch.setattr(dashboard, 'DATA_DIR', external)
    monkeypatch.setenv('SYNC_BUNDLED_DATA_ON_STARTUP', '1')

    state = dashboard.sync_bundled_data_if_newer()

    assert state['status'] == 'skipped'
    assert state['files_copied'] == []
    assert (external / 'sample.csv').read_text(encoding='utf-8') == 'date,value\n2026-06-01,2\n'


def test_data_dir_version_uses_metadata_and_spr_timestamp(tmp_path):
    data_dir = tmp_path / 'data'
    write_metadata(data_dir, '2026-05-01T12:00:00')
    (data_dir / 'spr_release_summary.csv').write_text(
        'metric,value,refreshed_at\nlatest,1,2026-06-02T09:30:00\n',
        encoding='utf-8',
    )

    assert dashboard.data_dir_version(data_dir) == datetime(2026, 6, 2, 9, 30)
