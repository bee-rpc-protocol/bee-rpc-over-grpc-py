from setuptools import setup, find_packages

setup(
    name='bee-rpc',
    version='0.0.1',

    url='https://github.com/bee-rpc-protocol/bee-rpc-over-grpc-py.git',

    py_modules=[
        'bee_rpc'
    ],
    install_requires=[
        'grpcio==1.56.0',
        # Not pinned to 4.23.3 any more: this package no longer reads the
        # FieldDescriptor.label attribute protobuf removed in 7.x, so it works across
        # the range. See bee_rpc.utils.is_repeated_message_field.
        'protobuf>=4.23.3',
    ],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.11",
)
