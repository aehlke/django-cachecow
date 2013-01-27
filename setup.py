from setuptools import find_packages, setup
import os


setup(
    name='django-cachecow',
    version='0.2.4',
    description='Cache decorators with namespaced invalidation, for Django.',
    #long_description=open('README').read(),

    # Get more strings from http://www.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
        "Framework :: Django",
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",        
        "Operating System :: OS Independent",        
        "License :: OSI Approved :: BSD License"
    ],
    keywords='django app cache caching memcache memcached',
    author='Alex Ehlke',
    author_email='alex.ehlke@gmail.com',
    url='http://github.com/aehlke/django-cachecow',
    license='BSD',
    packages=find_packages(exclude=['ez_setup']),
    namespace_packages=[],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'Django',
    ],
)

