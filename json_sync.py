from gluon.dal import DAL, Field, DEFAULT as DAL_DEFAULT
from gluon.storage import Storage
import json
import dateutil.parser
from dateutil.tz import tzutc, tzlocal
import datetime
import re
import inspect

__all__ = ['JSONRegistry', 'JSONField']

class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

# TODO support extra context for computed fields, like knockout $root, $parent etc.
# http://knockoutjs.com/documentation/binding-context.html

# TODO support types without a key (think nested objects belonging to the parent object)
# this should automatically delete orphaned nodes

# TODO work out how to handle keys better (don't just assume single integer primary key)

# TODO more efficient bulk updates / inserts (currently we try update then insert if count is 0)

# TODO support truncate + bulk insert to update all data in table & know there's no updates

# TODO support common fields on multiple types (think audit log signature in all tables)

def _ga(*args): return getattr(*args)

class JSONRegistry(object):
    __getitem__ = _ga

    def __init__(self):
        self.types = []

    def define_type(self, name, *fields, **kwargs):
        type = JSONType(self, name, *fields, **kwargs)
        self.types.append(type)
        return type

    def type_registry(self, db):
        if 'json_type_registry' not in db:
            db.define_table('json_type_registry',
                Field('type', notnull=True),
                Field('fieldname', notnull=True),
                Field('column_name'),
                Field('db_type', notnull=True),
                migrate=True,
                )
        return db.json_type_registry

    def fields_by_name(self, db, type):
        fields_by_name = {f.fieldname: f for f in type.fields}
        for extra_field in db(self.type_registry(db).type == type.name).select():
            if extra_field.fieldname in fields_by_name:
                # potentially migrate data here (esp. datetime)
                pass
            else:
                fields_by_name[extra_field.fieldname] = JSONField(extra_field.fieldname, extra_field.db_type)
        return fields_by_name

    def redefine_table(self, db, type):
        fields = []
        for field in self.fields_by_name(db, type).values():
            fields.append(field.field(self))
        db.define_table(type.table_name, *fields, migrate=True, redefine=True)

    def redefine_tables(self, db):
        for type in self.types:
            self.redefine_table(db, type)
    define_tables = redefine_tables

    def add_fields_to_type(self, db, type, fields):
        if not fields: return
        rows = []
        for fieldname, db_type in fields.items():
            rows.append(dict(type=type.name, fieldname=fieldname, db_type=db_type))
        self.type_registry(db).bulk_insert(rows)
        self.redefine_table(db, type)

    def __getattr__(self, name):
        for type in self.types:
            if type.name == name: return type
        raise AttributeError("%r object has no attribute %r" % (self.__class__, name))

class Context(object):
    def __init__(self, parent_context, type, data=None, seq=None, partial=False):
        self.parent_context = parent_context
        self.type = type
        self.data = data
        self.seq = seq
        self.partial = partial
        self.index = -1
        if parent_context is not None:
            self.parents = [parent_context.data] + parent_context.parents
            self.parent_contexts = [parent_context] + parent_context.parent_contexts
            self.root = parent_context.root
            self.root_context = parent_context.root_context
        else:
            self.parents = []
            self.parent_contexts = []
            self.root = data
            self.root_context = self

class JSONType(object):
    __getitem__ = _ga

    default_attributes = {
        'remove_missing_fields': True,
        'table_name': None,
        }
    def __init__(self, registry, name, *fields, **kwargs):
        for attr, value in JSONType.default_attributes.items():
            self.__dict__[attr] = kwargs.get(attr, value)
        if set(kwargs) - set(JSONType.default_attributes):
            raise Exception('Extra kwargs set: %r' % (set(kwargs) - set(JSONType.default_attributes)))
        self._registry = registry
        self.name = name
        if self.table_name is None:
            self.table_name = name
        self.fields = list(fields)
        self.fields_by_name = {field.fieldname: field for field in self.fields}

    def __getattr__(self, name):
        if name in self.fields_by_name:
            return self.fields_by_name[name]
        raise AttributeError("%r object has no attribute %r" % (self.__class__, name))

    def _find_extra_types(self, current_fields, missing_fields, obj):
        for fieldname in obj:
            if fieldname in current_fields: continue
            if fieldname == 'id': continue
            value = obj[fieldname]
            if value is None: continue
            missing_fields.setdefault(fieldname, set()).add(type(value))

    def _redefine_with_missing_fields(self, db, missing_fields):
        db_types = {}
        for fieldname, types in missing_fields.items():
            db_type = None
            if len(types) == 1:
                _type = next(iter(types))
                if _type in {int, long}:
                    db_type = 'integer'
                elif _type in {dict, list, tuple}:
                    # TODO maybe support set, tuple, other iterables?
                    db_type = 'json'
                elif _type is bool:
                    db_type = 'boolean'
                elif _type is float:
                    db_type = 'double'
            if db_type is None: db_type = 'string'
            db_types[fieldname] = db_type
        self._registry.add_fields_to_type(db, self, db_types)

    def _create_row_dict(self, db, context):
        row = AttrDict()
        for fieldname in context.data:
            if fieldname in self.fields_by_name: continue
            value = context.data[fieldname]
            if value is not None or fieldname in db[self.table_name].fields:
                row[fieldname] = context.data[fieldname]
        for field in self.fields:
            if field.compute:
                if len(inspect.getargspec(field.compute).args) == 1:
                    value = field.compute(row)
                else:
                    value = field.compute(row, context)
                row[field.column_name] = value
                continue
            if context.partial and field.fieldname not in context.data:
                continue
            try:
                value = context.data[field.fieldname]
            except KeyError:
                value = None
            if value is None:
                row[field.column_name] = None
                continue
            if field.type in {'datetime', 'date', 'time'}:
                try:
                    if field.date_format:
                        value = datetime.datetime.strptime(value, field.date_format)
                    else:
                        dateutil_kwargs = field.dateutil_kwargs or {}
                        value = dateutil.parser.parse(value, **dateutil_kwargs)
                except ValueError, e:
                    print 'Error parsing date string %r' % value
                    raise e
                if value.tzinfo:
                    value.astimezone(tzlocal())
                if field.type == 'date':
                    value = value.date()
                if field.type == 'time':
                    value = value.time()
            ref_match = re.match(r'^(reference|list:reference) (\w+)$', field.type)
            if ref_match and ref_match.group(1) == 'reference':
                ref_type = self._registry[ref_match.group(2)]
                if type(value) not in {int, long}:
                    new_context = Context(context, ref_type, data=AttrDict(value), partial=context.partial)
                    child = ref_type._sync(db, new_context)
                    # FIXME assumes ID
                    value = long(child['id'])
            elif ref_match and ref_match.group(1) == 'list:reference':
                ref_type = self._registry[ref_match.group(2)]
                if type(value) not in {list, tuple}:
                    value = [value]
                ids = [None] * len(value)
                seq = []
                indexes = []
                for i, item in enumerate(value):
                    if type(item) in {int, long}:
                        ids[i] = item
                    else:
                        seq.append(item)
                        indexes.append(i)
                if seq:
                    new_context = Context(context, ref_type, seq=seq, partial=context.partial)
                    children = ref_type._bulk_sync(db, new_context)
                    # FIXME assumes ID
                    for i, child in enumerate(children):
                        ids[indexes[i]] = long(child['id'])
                value = ids
            row[field.column_name] = value
        return row

    def _update_row(self, db, row_dict):
        table = db[self.table_name]
        # FIXME this assumes id column + in JSON, works for PCO
        db_row = table(row_dict['id'])
        if db_row:
            if self.remove_missing_fields:
                new_row_dict = None
                for column in db_row:
                    if column not in row_dict:
                        if new_row_dict is None:
                            new_row_dict = AttrDict(**row_dict)
                        new_row_dict[column] = None
                if new_row_dict is not None:
                    row_dict = new_row_dict
            db_row.update_record(**row_dict)
            return True
        return False

    def _sync(self, db, context):
        current_fields = self._registry.fields_by_name(db, self)
        missing_fields = {}
        self._find_extra_types(current_fields, missing_fields, context.data)
        self._redefine_with_missing_fields(db, missing_fields)
        row_dict = self._create_row_dict(db, context)
        if not self._update_row(db, row_dict):
            db[self.table_name].insert(**row_dict)
        return row_dict

    def _bulk_sync(self, db, context):
        current_fields = self._registry.fields_by_name(db, self)
        missing_fields = {}
        for obj in context.seq:
            self._find_extra_types(current_fields, missing_fields, obj)
        self._redefine_with_missing_fields(db, missing_fields)
        bulk_insert_row_dicts = []
        all_row_dicts = []
        for index, obj in enumerate(context.seq):
            context.index = index
            context.data = AttrDict(obj)
            row_dict = self._create_row_dict(db, context)
            all_row_dicts.append(row_dict)
            if not self._update_row(db, row_dict):
                bulk_insert_row_dicts.append(row_dict)
        if bulk_insert_row_dicts:
            db[self.table_name].bulk_insert(bulk_insert_row_dicts)
        return all_row_dicts

    def sync(self, db, obj, partial=False):
        context = Context(None, self, data=AttrDict(obj), partial=partial)
        return self._sync(db, context)

    def bulk_sync(self, db, seq, partial=False):
        context = Context(None, self, seq=seq, partial=partial)
        return self._bulk_sync(db, context)

class JSONField(object):
    our_attributes = {'date_format', 'dateutil_kwargs', 'compute', 'column_name'}
    def __init__(self, fieldname, type='string', *args, **kwargs):
        for attr in JSONField.our_attributes:
            self.__dict__[attr] = None
        self.fieldname = fieldname
        self.column_name = fieldname
        self.type = type
        self.field_kwargs = dict(**kwargs)
        self.field_args = list(args)
        for key in kwargs:
            if key in JSONField.our_attributes:
                del self.field_kwargs[key]
                self.__dict__[key] = kwargs[key]
        # check we can actually create the Field
        self.field()

    def field(self, registry=None):
        type = self.type
        if registry:
            ref_match = re.match(r'^(reference|list:reference) (\w+)$', type)
            if ref_match:
                ref_type = registry[ref_match.group(2)]
                type = ref_match.group(1) + ' ' + ref_type.table_name
        return Field(self.column_name, type, *self.field_args, **self.field_kwargs)
