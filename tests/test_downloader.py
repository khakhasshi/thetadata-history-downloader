import contextlib
import io
import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import batch_download_clean_1m as batch
import update_1m_data_to_latest as update
import verify_downloads


def bars(day='2026-10-01'):
    return pd.DataFrame({'timestamp': pd.date_range(day+' 09:30', periods=2, freq='min', tz='America/New_York'), 'open': [100., 101.], 'high': [102., 103.], 'low': [99., 100.], 'close': [101., 102.], 'volume': [100, 200]})


class DataContract(unittest.TestCase):
    def test_month_chunks_leap_and_boundary(self):
        self.assertEqual(list(batch.month_chunks(date(2024,2,28),date(2024,3,2))), [(date(2024,2,28),date(2024,2,29)),(date(2024,3,1),date(2024,3,2))])

    def test_symbols_reject_path_traversal(self):
        self.assertEqual(batch.dedupe_symbols(['spy',' SPY ','nvdl']), ['SPY','NVDL'])
        with self.assertRaises(ValueError): batch.dedupe_symbols(['../secret'])

    def test_invalid_prices_removed_and_bounds_rejected(self):
        f=bars();f.loc[0,'open']=0
        self.assertEqual(len(batch.clean_bars(f)),1)
        f=bars();f.loc[0,'high']=90
        with self.assertRaises(ValueError):batch.clean_bars(f)

    def test_fixed_offset_preserves_winter_wall_clock(self):
        f=batch.normalize_to_fixed_edt(bars('2026-01-02'))
        self.assertEqual(f.timestamp.iloc[0].hour,9)
        self.assertEqual(f.timestamp.iloc[0].utcoffset().total_seconds(),-14400)

    def test_retry_does_not_emit_exception_payload(self):
        out=io.StringIO()
        with contextlib.redirect_stdout(out),patch.object(batch.time,'sleep'),patch.object(batch.random,'uniform',return_value=0):
            with self.assertRaises(RuntimeError):
                batch.with_retries('test',lambda:(_ for _ in ()).throw(RuntimeError('Connection secret-token')),2,0,0)
        self.assertNotIn('secret-token',out.getvalue())

    def test_relocated_incremental_dataset_and_empty_response(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);folder=root/'SPY';folder.mkdir()
            old=batch.normalize_to_fixed_edt(bars())
            paths=batch.write_frame(old,folder/'SPY_1m_unadjusted_clean_edt_20261001_20261001',True,True)
            meta={'symbol':'SPY','start_date':'2026-10-01','end_date':'2026-10-01','outputs':{'clean_edt':{k:'/old/location/'+Path(v).name for k,v in paths.items()}}}
            m=folder/'SPY_1m_metadata_20261001_20261001.json';m.write_text(json.dumps(meta))
            with patch.object(sys,'argv',['update','--output-dir',str(root),'--end-date','20261002']):args=update.parse_args()
            before=m.read_bytes()
            with patch.object(update,'download_clean_range',return_value=(pd.DataFrame(),1,0,0)):
                r=update.update_symbol(args,'SPY',{'client':None},lambda:None)
            self.assertEqual(r['status'],'no_new_rows');self.assertEqual(m.read_bytes(),before)
            delta=batch.normalize_to_fixed_edt(bars('2026-10-02'))
            with patch.object(update,'download_clean_range',return_value=(delta,1,1,2)):
                r=update.update_symbol(args,'SPY',{'client':None},lambda:None)
            new=pd.read_parquet(r['outputs']['clean_edt']['parquet'])
            pd.testing.assert_frame_equal(new.iloc[:len(old)].reset_index(drop=True),old)
            self.assertEqual(len(new),4)
            report=verify_downloads.verify(root,expected_end='2026-10-02')
            self.assertEqual(report['status'],'passed')
            new=pd.concat([new,new.iloc[:1]],ignore_index=True);new.to_parquet(r['outputs']['clean_edt']['parquet'],index=False)
            self.assertEqual(verify_downloads.verify(root)['status'],'failed')


if __name__=='__main__':unittest.main()
