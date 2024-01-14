#!/usr/bin/env python
'''
Development script to re-build the documentation pages.

The main API documentation is done using pdoc (https://pdoc.dev/)

The other minor bits are done with Markdown files which GitHub Pages
will render. The page with the command line help is generated here
by combining the output of 'moamosaic -h' with cmdline.md.template

After running this script, anything which changed (see 'git diff') should
be committed to the repository. When the changes are then pushed to github,
they will appear on the Pages site.
'''
import sys
import os
import subprocess
import shutil


DOCDIR = 'docs'
APIDIR = 'api'
CMDLINE_MD = 'cmdline.md'
CMDLINE_TEMPLATE = '{}.template'.format(CMDLINE_MD)


def main():
    """
    Main routine
    """
    scriptDir = os.path.dirname(sys.argv[0])
    # Are these always the same? Probably......
    docDir = scriptDir

    # Re-build all the pdoc API pages
    apiDocDir = os.path.join(docDir, APIDIR)
    if os.path.exists(apiDocDir):
        shutil.rmtree(apiDocDir)
    pdocCmd = ['pdoc', '--docformat', 'numpy', '--no-search',
                '-o', apiDocDir, 'moamosaic']
    proc = subprocess.Popen(pdocCmd)

    # Re-create cmdline.md
    templateFile = os.path.join(docDir, CMDLINE_TEMPLATE)
    templateStr = open(templateFile).read()
    # Get the command line help
    proc = subprocess.Popen(['moamosaic', '-h'], stdout=subprocess.PIPE,
        universal_newlines=True)
    (stdout, stderr) = proc.communicate()
    cmdlineHelp = stdout
    # Substitute into the template
    cmdlineMdStr = templateStr.replace('$CMDLINEHELP', cmdlineHelp)
    open(CMDLINE_MD, 'w').write(cmdlineMdStr)


if __name__ == "__main__":
    main()
