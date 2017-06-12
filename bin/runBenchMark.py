#!/usr/bin/python




if __name__ == '__main__':
    args = sys.argv
    if len(args) < 4:
        usage()
    component = args[1]
    actions = args[2]
    conf_p = args[3]

