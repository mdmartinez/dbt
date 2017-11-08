import json
import os.path

from dbt.adapters.factory import get_adapter
from dbt.compat import basestring

import dbt.clients.jinja
import dbt.context.common
import dbt.flags
import dbt.utils

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


execute = True


def ref(model, project, profile, flat_graph):
    current_project = project.get('name')

    def do_ref(*args):
        target_model_name = None
        target_model_package = None

        if len(args) == 1:
            target_model_name = args[0]
        elif len(args) == 2:
            target_model_package, target_model_name = args
        else:
            dbt.exceptions.ref_invalid_args(model, args)

        target_model = dbt.parser.resolve_ref(
            flat_graph,
            target_model_name,
            target_model_package,
            current_project,
            model.get('package_name'))

        if target_model is None:
            dbt.exceptions.ref_target_not_found(
                model,
                target_model_name,
                target_model_package)

        target_model_id = target_model.get('unique_id')

        if target_model_id not in model.get('depends_on', {}).get('nodes'):
            dbt.exceptions.ref_bad_context(model,
                                           target_model_name,
                                           target_model_package)

        if dbt.utils.get_materialization(target_model) == 'ephemeral':
            model['extra_ctes'][target_model_id] = None

        adapter = get_adapter(profile)
        return dbt.utils.Relation(profile, adapter, target_model)

    return do_ref


class Config:
    def __init__(self, model):
        self.model = model

    def __call__(*args, **kwargs):
        return ''

    def set(self, name, value):
        return self.__call__({name: value})

    def _validate(self, validator, value):
        validator(value)

    def require(self, name, validator=None):
        if name not in self.model['config']:
            dbt.exceptions.missing_config(self.model, name)

        to_return = self.model['config'][name]

        if validator is not None:
            self._validate(validator, to_return)

        return to_return

    def get(self, name, validator=None, default=None):
        to_return = self.model['config'].get(name, default)

        if validator is not None and default is not None:
            self._validate(validator, to_return)

        return to_return


def generate(model, project, flat_graph):
    return dbt.context.common.generate(
        model, project, flat_graph, dbt.context.runtime)


def _set_attr_in(d, path, value):
    if not path:
        return value

    first, rest = path[0], path[1:]

    if not d.get(first):
        d[first] = dbt.utils.AttrDict()

    d[first] = _set_attr_in(d[first], rest, value)

    return d


def import_file(flat_graph, context):
    def fn(path, alias=None):
        path_in_context = None
        for _, node in flat_graph.get('macros').items():
            node_path = node.get('original_file_path')
            node_path = os.path.splitext(node_path)[0]
            node_path = node_path.split(os.sep)

            if path.split('.') == node_path:
                if alias is None:
                    path_in_context = path.split('.')
                else:
                    path_in_context = [alias]

                _set_attr_in(context,
                             path_in_context,
                             node.get('module'))

                return ''

        dbt.exceptions.raise_compiler_error(
            "Failed to import {}".format(path))

    return fn
