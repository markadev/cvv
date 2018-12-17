from setuptools import find_packages, setup


setup(
    name='cvv',
    version='0.1',
    description='Concise Version Vectors',
    author='Mark Aikens',
    author_email='markadev@primeletters.net',
    license='MIT',

    packages=find_packages('src'),
    package_dir={'': 'src'},
)
