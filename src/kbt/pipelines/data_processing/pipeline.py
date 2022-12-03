"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.3
"""

from kedro.pipeline import Pipeline, node, pipeline
from kedro.config import ConfigLoader
from .nodes import preprocess_companies, preprocess_shuttles
from pathlib import Path
import kbt.settings as settings
import jinja2
import duckdb
import pandas as pd
from inspect import signature, Parameter
from typing import Any
import json


def parse_sql_file(sql_file: Path, parameters: dict[str, Any]):
    inputs = []
    def source_func(table) -> str:
        if table not in inputs:
            inputs.append(table)
        # file = next(settings.DATA_DIR/folder).rglob(f'{table}.*')
        # return f'{folder}.{table}'
        return table

    def ref_func(table) -> str:
        if table not in inputs:
            inputs.append(table)
        return table
    
    def param_func(param):
        k = f'params:{param}'
        if k not in inputs:
            inputs.append(k)
        return json.dumps(parameters[param])

    raw_sql = jinja2.Template(sql_file.read_text()).render(
        {
            'source': source_func,
            'ref': ref_func,
            'param': param_func,
            **parameters,
        })

    def run_sql(**kwargs) -> pd.DataFrame:
        duckdb.paramstyle = 'named'
        con = duckdb.connect(database=':memory:')
        for k, v in kwargs.items():
            if isinstance(v, pd.DataFrame):
                con.register(k, v)
        print(raw_sql)
        result = con.execute(raw_sql)
        return result.df()
    sig = signature(run_sql)
    sig = sig.replace(parameters=[
        Parameter(name=k, kind=Parameter.POSITIONAL_OR_KEYWORD, annotation=pd.DataFrame)
        for k in inputs
        if not k.startswith('params:')
    ] + [Parameter(name='parameters', kind=Parameter.POSITIONAL_OR_KEYWORD, annotation=dict)])
    run_sql.__signature__ = sig
    return {
        'name': f'{sql_file.stem}_node',
        'func': run_sql,
        'inputs': {**{x: x for x in inputs if not x.startswith('params:')}, 'parameters': 'parameters'},
        'outputs': sql_file.stem,
    }


def create_node_from_sql_file(sql_file: Path, parameters: dict[str, Any]):
    info = parse_sql_file(sql_file, parameters)
    return node(**info)


def create_pipeline(**kwargs) -> Pipeline:
    conf_path = str(settings.CONF_DIR)
    conf_loader = ConfigLoader(conf_source=conf_path)
    parameters = conf_loader.get("parameters*", "parameters*/**")
    nodes = [
        create_node_from_sql_file(sql_file, parameters)
        for sql_file in settings.SQL_DIR.rglob('*.sql')
    ]
    return pipeline(
        [
            node(
                func=preprocess_companies,
                inputs="companies",
                outputs="preprocessed_companies",
                name="preprocess_companies_node",
            ),
            node(
                func=preprocess_shuttles,
                inputs="shuttles",
                outputs="preprocessed_shuttles",
                name="preprocess_shuttles_node",
            ),
            *nodes
        ]
    )