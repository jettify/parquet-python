try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from Cython.Build import cythonize
from distutils.extension import Extension

sourcefiles = ['parquet/_optimized.pyx', 'parquet/optimized.c']

extensions = [Extension("parquet._optimized", sourcefiles)]

setup(
    name='parquet',
    version='1.0.0a2',
    description='Python support for Parquet file format',
    author='Joe Crobak',
    author_email='joecrow@gmail.com',
    packages=['parquet'],
    install_requires=[
        'thriftpy', 'cython', 'pandas',
    ],
    tests_require=['pytest',],
    extras_require={
        'snappy support': ['python-snappy']
    },
    entry_points={
        'console_scripts': [
            'parquet = parquet.__main__:main',
        ]
    },
    ext_modules=cythonize(extensions)
)
