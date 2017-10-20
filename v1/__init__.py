# -*- coding: utf-8 -*-
import datetime
import logbook
import pandas as pd

import biglearning.module2.common.interface as I
from biglearning.api import M
from biglearning.module2.common.data import Outputs, DataSource
from biglearning.module2.common.utils import smart_object


bigquant_cacheable = True

# 模块接口定义
bigquant_category = '机器学习'
bigquant_friendly_name = '滚动运行'
bigquant_doc_url = 'https://bigquant.com/docs/'
log = logbook.Logger(bigquant_friendly_name)


def bigquant_run(
    run: I.port('训练，训练模块的延迟执行输出'),
    input_list: I.port('输入数据(列表)'),
    param_name: I.str('参数名，run 中用于接收滚动运行数据的参数。示例1：rolling_input；示例2：run_a=rolling_b|run_c=rolling_d。具体见源代码')='rolling_input') -> [
        I.port('输出数据(列表)', 'data')
    ]:
    '''
    通用滚动运行。简单的可以理解为 map(run, input_list)
    '''
    run_data = smart_object(run)
    module_run = M.m_get_module(run_data['name']).m_get_version(run_data['version'])
    module_kwargs = run_data['kwargs']

    param_mapping = [s.strip().split('=') for s in param_name.strip().split('|')]
    param_mapping = [s[:2] if len(s) >= 2 else (s[0], None) for s in param_mapping]

    input_list_data = smart_object(input_list)
    rollings = []
    for item in input_list_data:
        for k, v in param_mapping:
            module_kwargs[k] = item[v] if v else item
        item['output'] = module_run(**module_kwargs)
        rollings.append(item)

    ds = DataSource.write_pickle(rollings, use_dill=True)
    return Outputs(data=ds)


def bigquant_postrun(outputs):
    return outputs


if __name__ == '__main__':
    # 测试代码
    pass
