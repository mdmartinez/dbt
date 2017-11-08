import dbt.utils
import dbt.exceptions

import dbt.context.common

from dbt.adapters.factory import get_adapter


execute = False


def ref(model, project, profile, flat_graph):

    def ref(*args):
        if len(args) == 1 or len(args) == 2:
            model['refs'].append(args)

        else:
            dbt.exceptions.ref_invalid_args(model, args)

        adapter = get_adapter(profile)
        return dbt.utils.Relation(profile, adapter, model)

    return ref


class Config:
    def __init__(self, model):
        self.model = model

    def __call__(self, *args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0:
            opts = args[0]
        elif len(args) == 0 and len(kwargs) > 0:
            opts = kwargs
        else:
            dbt.exceptions.raise_compiler_error(
                "Invalid inline model config",
                self.model)

        self.model['config_reference'].update_in_model_config(opts)
        return ''

    def set(self, name, value):
        return self.__call__({name: value})

    def require(self, name, validator=None):
        return ''

    def get(self, name, validator=None, default=None):
        return ''


def generate(model, project, flat_graph):
    return dbt.context.common.generate(
        model, project, flat_graph, dbt.context.parser)


def _set_attr_in(d, path, value):
    if not path:
        return value

    first, rest = path[0], path[1:]

    print('d')
    print(first)
    print(rest)

    if not d.get(first):
        d[first] = dbt.utils.AttrDict()

    d[first] = _set_attr_in(d[first], rest, value)

    return d


def import_file(flat_graph, context):
    def fn(path):
        _set_attr_in(context,
                     path.split('.'),
                     dbt.utils.AttrDict())

        return ''

    return fn
