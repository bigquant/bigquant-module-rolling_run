# -*- coding: utf-8 -*-
import datetime
import logbook
import pandas as pd

import biglearning.module2.common.interface as I
from biglearning.api import M
from biglearning.module2.common.data import Outputs, DataSource


log = logbook.Logger('rolling_run')
bigquant_cacheable = True

# 模块接口定义
bigquant_category = '机器学习'
bigquant_friendly_name = '滚动运行'
bigquant_doc_url = 'https://bigquant.com/docs/'


def bigquant_run(
    train_run: I.port('训练，训练模块的延迟执行输出'),
    input_list: I.port('数据列表'),
    param_name: I.str('参数名，train_run 中用于接收滚动运行数据的参数')='rolling_input') -> [
        I.port('证券数据', 'data')
    ]:
    '''
    通用滚动运行。简单的可以理解为 map(train_run, input_list)
    '''
    train_run_data = train_run.read_pickle()
    module_run = M.m_get_module(train_run_data['name']).m_get_version(train_run_data['version'])
    module_kwargs = train_run_data['kwargs']

    input_list_data = input_list.read_pickle()
    rollings = []
    for item in input_list_data:
        log.info('运行, %s, %s' % (item['start_date'], item['end_date']))
        module_kwargs[param_name] = item
        item['output'] = module_run(**module_kwargs)
        rollings.append(item)

    ds = DataSource.write_pickle(rollings, use_dill=True)
    return Outputs(data=ds)


def bigquant_postrun(outputs):
    return outputs


if __name__ == '__main__':
    # 测试代码
    pass
