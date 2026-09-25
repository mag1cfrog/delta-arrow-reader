"""Do not conflate nested division, arbitrary errors or successful NULLs."""
import json
from pathlib import Path
import tempfile
import unittest
from modulo_null_guard import compare


class ModuloCauseTest(unittest.TestCase):
    def test_error_identity_and_direct_operator_context(self):
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory)
            case={'id':'q','sql':'SELECT a % b','direct_modulo':True}
            def write_capture(name,actual):
                (p/name).write_text(json.dumps({'cases':[case],'results':[{'id':'q_'+mode,'actual':actual} for mode in ['true','false']]}))
            def run(actual):
                (p/'cases.jsonl').write_text(json.dumps(case)+'\n')
                write_capture('spark.json',{'status':'execution_error','condition':'REMAINDER_BY_ZERO','error':'Remainder by zero'})
                write_capture('native.json',actual)
                return compare(p/'spark.json',p/'native.json',p/'cases.jsonl')
            native={'status':'execution_error','error':'Arrow error: Divide by zero error'}
            self.assertEqual(run(native)['agreement'],2)
            case['direct_modulo']=False
            self.assertEqual(run(native)['agreement'],0)
            case['direct_modulo']=True
            for actual in [{'status':'execution_error','error':'Execution error: Underflow'},
                           {'status':'execution_error','error':'unknown'},
                           {'status':'ok','types':['Int32'],'rows':[[None]]}]:
                self.assertEqual(run(actual)['agreement'],0)


if __name__=='__main__':unittest.main()
