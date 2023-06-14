from setuptools import setup, find_packages

with open('README.md', 'r') as f:
    readme = f.read()

setup(
    name='anysql',
    version='0.0.2',
    license='MIT',
    packages=find_packages(),
    url='https://github.com/imgurbot12/anysql',
    author='Andrew Scott',
    author_email='imgurbot12@gmail.com',
    description='Lightweight, Thread-Safe, Version-Agnostic, SQL Client Implementation',
    python_requires='>=3.7',
    long_description=readme,
    long_description_content_type="text/markdown",
    package_data={"anysql": ["py.typed"]},
    install_requires=[
        'dataclasses',
        'contextvars',
        'typing_extensions',
    ],
    extras_require={
        "mysql":      ["pymysql"],
        "postgresql": ["psycopg2-binary"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
