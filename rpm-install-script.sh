python setup.py install --single-version-externally-managed --root="$RPM_BUILD_ROOT" --record=INSTALLED_FILES

# Add .pyo files generated on CentOS 5.x
# This is a workaround until someone comes with a better fix, like avoiding
# /usr/lib/rpm/brp-python-bytecompile to run or convincing people that CentOS is pure evil.
(cd "$RPM_BUILD_ROOT" && find -type f -name '*.pyc' | cut -b2-|sed -e 's/pyc$/pyo/') >> INSTALLED_FILES
sort -u INSTALLED_FILES > .INSTALLED_FILES
mv .INSTALLED_FILES INSTALLED_FILES
