def rename_files():

    import os
    from os import listdir
    from os.path import isfile, join

    mypath = '/home/shane/Downloads/prank/prank'
    onlyfiles = [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]

    for file in onlyfiles:
        resultFile = ''.join([i for i in file if not i.isdigit()])
        os.rename(mypath+ "/"+ file, mypath+ "/"+ resultFile)
        print resultFile


rename_files()
