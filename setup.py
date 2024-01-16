from setuptools import setup, find_packages



# Populate long description setting with content of README
#
# Use markdown format read me file as GitHub will render it automatically
# on package page
with open("README.md") as readme_file:
    _long_description = readme_file.read()
    
    
setup(
    name='compositenimroddata',
    version='1.0.0',
    description='Composite NIMROD data in HDF5 format',
    author='Zach Wharton',
    author_email='zach.wharton@stfc.ac.uk',
    url='https://github.com/ncasuk/nimrod_hdf5_aggregator/tree/ceda-ingest',
    long_description=_long_description,
    long_description_content_type='text/markdown',
    license='BSD - See ceda_example/LICENSE file for details',
    packages=find_packages(),
    package_data={
        'ceda_example': [
            'LICENSE',
        ],
    },
    install_requires=['h5py', 'numpy'],
    
    # This qualifier can be used to selectively exclude Python versions - 
    # in this case early Python 2 and 3 releases
    python_requires='>=3.5.0', 
    
    # See:
    # https://www.python.org/dev/peps/pep-0301/#distutils-trove-classification
    classifiers=[
        'Development Status :: 3',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Security',
        'Topic :: Internet',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Systems Administration :: Authentication/Directory',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            ],
        }
)