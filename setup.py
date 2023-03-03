import setuptools

with open('README.md', 'r') as f:
    long_description = f.read()
with open('requirements.txt', 'r') as f:
    requirements = f.read().strip('\n').split('\n')

entry_points = {
    'console_scripts': [
        'kb_indexer=indexers.entrypoint:cli',
        ]
    }

setuptools.setup(
    name='kb-indexer',
    version='2023.03.03',
    description='Indexer for the ENVRI-FAIR knowledge base',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/QCDIS/kb-indexer',
    entry_points=entry_points,
    packages=setuptools.find_packages(),
    python_requires='>=3.9',
    install_requires=requirements,
    )
