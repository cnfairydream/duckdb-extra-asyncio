# -*- coding: utf-8 -*-

def delegate_to_agent(*attrs):
    def cls_builder(cls):
        for attr_name in attrs:
            setattr(cls, attr_name, _make_delegate_method(attr_name))
        return cls

    return cls_builder


def proxy_method_directly(*attrs):
    def cls_builder(cls):
        for attr_name in attrs:
            setattr(cls, attr_name, _make_proxy_method(attr_name))
        return cls

    return cls_builder


def proxy_property_directly(*attrs):
    def cls_builder(cls):
        for attr_name in attrs:
            setattr(cls, attr_name, _make_proxy_property(attr_name))
        return cls

    return cls_builder


def _make_delegate_method(attr_name):
    async def method(self, *args, **kwargs):
        return await self._execute(getattr(self._connection, attr_name), *args, **kwargs)

    return method


def _make_proxy_method(attr_name):
    def method(self, *args, **kwargs):
        return getattr(self._connection, attr_name)(*args, **kwargs)

    return method


def _make_proxy_property(attr_name):
    def proxy_property(self):
        return getattr(self._connection, attr_name)

    return property(proxy_property)
