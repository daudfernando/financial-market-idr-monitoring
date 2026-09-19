import unittest
import tempfile
from pathlib import Path
import json
from scripts.market_refresh import merge_bars, latest_sessions, activate


class RefreshTests(unittest.TestCase):
    def test_fresh_clone_creates_runtime_config_without_seed(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            (root/'config').mkdir()
            folder=root/'data/market_universe/20260919T000000000000Z'
            folder.mkdir(parents=True)
            rows=[{'symbol':s,'bar_start_utc':'2026-09-18T19:59:00+00:00','price_usd':100}
                  for s in ['BAC','GS','JPM','MS']]
            result=activate(root,folder,rows)
            self.assertEqual(result['added'],4)
            config=json.loads((root/'config/current_market_replay.json').read_text())
            self.assertTrue((root/config['input']).is_file())
            self.assertFalse((root/'data/market_universe/refresh.lock').exists())
    def test_repeat_does_not_duplicate(self):
        row={'symbol':'JPM','bar_start_utc':'2026-09-16T13:30:00+00:00','price_usd':100}
        merged,added,revised=merge_bars([row],[dict(row)])
        self.assertEqual((merged,added,revised),([row],0,0))

    def test_new_date_preserved_revision_replaces_same_key(self):
        old={'symbol':'JPM','bar_start_utc':'2026-09-16T13:30:00+00:00','price_usd':100}
        revised={**old,'price_usd':101}
        new={**old,'bar_start_utc':'2026-09-17T13:30:00+00:00'}
        merged,added,changed=merge_bars([old],[revised,new])
        self.assertEqual((len(merged),added,changed),(2,1,1))
        self.assertEqual(merged[0]['price_usd'],101)

    def test_latest_window_excludes_oldest_session(self):
        rows=[{'symbol':s,'bar_start_utc':f'2026-09-{d:02}T13:30:00+00:00'}
              for d in [10,11,14,15,16,17] for s in ['JPM','BAC','GS','MS']]
        active=latest_sessions(rows)
        self.assertEqual(len(active),20)
        self.assertTrue(all(not r['bar_start_utc'].startswith('2026-09-10') for r in active))

    def test_duplicate_source_refused(self):
        row={'symbol':'JPM','bar_start_utc':'2026-09-16T13:30:00+00:00'}
        with self.assertRaises(ValueError):
            merge_bars([], [row,row])
