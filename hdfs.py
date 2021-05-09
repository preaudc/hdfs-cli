#!/usr/bin/python3

import argparse
import getpass
import json
import requests
import sys
import yaml

from datetime import datetime
from urllib.error import URLError
from urllib.request import BaseHandler, HTTPRedirectHandler, Request, build_opener, install_opener, urlopen

# http://gitlab.corp.kelkoo.net/pack-conf/shivaBuildParents/-/blob/master/spark-applications/shivabuild.ini
# https://docs.python.org/3/library/argparse.html#the-add-argument-method
# https://docs.python.org/3/library/urllib.request.html?highlight=urllib#http-error-nnn
# https://docs.python.org/3/library/email.compat32-message.html#email.message.Message
# https://stackoverflow.com/questions/4981977/how-to-handle-response-encoding-from-urllib-request-urlopen
# https://hadoop.apache.org/docs/r2.8.3/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_and_Write_to_a_File

# custom HTTPRedirectHandler for WebHDFS
class HdfsHTTPRedirectHandler(HTTPRedirectHandler):
    def __init__(self):
        super().__init__()

    def redirect_request(self, req, fp, code, msg, hdrs, newurl):
        if code == 307:
            #hdrs.set_charset("utf-8")
            #print(hdrs.get_content_type())
            #print(hdrs.get_charset())
            #print(hdrs.get_content_charset())
            #print(hdrs.get_charsets())
            #print(req.info().get_content_charset())
            #print(hdrs['Content-Type'])
            if req.data != None:
                # rewind because file-like object has already been read by first request
                req.data.seek(0)
            return Request(url = newurl, data = req.data, method = req.get_method()) 

# custom BaseHandler for WebHDFS
class HdfsBaseHandler(BaseHandler):
    def __init__(self):
        super().__init__()

    def http_error_403(self, req, fp, code, msg, hdrs):
        jsonData = json.loads(fp.read())
        sys.exit(jsonData['RemoteException']['message'])

    def http_error_404(self, req, fp, code, msg, hdrs):
        jsonData = json.loads(fp.read())
        sys.exit(jsonData['RemoteException']['message'])

def concat_path(path1, path2):
    if path1 == '':
        return path2
    elif path2 == '':
        return path1
    else:
        return path1.rstrip('/') + '/' + path2.lstrip('/')

def format_date(timestamp_ms):
    dt = datetime.utcfromtimestamp(timestamp_ms / 1000)
    return dt.strftime('%Y-%m-%d %H:%M')

# get active NameNode
def get_active_nn(nn_list):
    def is_active_nn(nn):
        jmx_url = "http://{host}:{port}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus".format(
            host = nn['Host'],
            port = nn['Port']
        )
        try:
            jsonData = json.loads(urlopen(jmx_url).read())
            return 'active' == jsonData['beans'][0]['State']
        except URLError as e:
            pass

    for nn in nn_list:
        if is_active_nn(nn):
            return nn
    sys.exit('ERROR: all NameNodes are DOWN!')

# transform permissions from octal to 'rwx' format
def get_str_perms(oct_perms):
    rwx_perm_def = 'rwx'
    rwx_perms = ''
    for oct_perm in oct_perms:
        bin_perm = "{0:b}".format(int(oct_perm))
        for i in range(0, len(bin_perm)):
            rwx_perms += rwx_perm_def[i] if int(bin_perm[i]) else '-'
    return rwx_perms

# implement 'hdfs dfs -cat'
def hdfs_cat(args, user, webhdfs_prefix_url, default_dir):
    abs_path = args.path if args.path.startswith('/') else concat_path(default_dir, args.path)
    webhdfs_url = "{prefix_url}/{path}?user.name={user}&op=OPEN".format(
        prefix_url = webhdfs_prefix_url,
        path = abs_path.lstrip('/'),
        user = user
    )
    # actual read is done by redirect_request in HdfsHTTPRedirectHandler
    data = urlopen(webhdfs_url).read()
    print(data.decode(sys.stdout.encoding))

# implement 'hdfs dfs -cp'
def hdfs_cp(args, user, webhdfs_prefix_url, default_dir):
    dst_path = args.path.pop()
    abs_dst_path = dst_path if dst_path.startswith('/') else concat_path(default_dir, dst_path)
    for src_path in args.path:
        dst_file = src_path.split('/').pop()
        webhdfs_url = "{prefix_url}/{path}?user.name={user}&op=CREATE".format(
            prefix_url = webhdfs_prefix_url,
            path = concat_path(abs_dst_path.lstrip('/'), dst_file),
            user = user
        )
        print(webhdfs_url)
        #file_data = open(src_path)
        #req = Request(url = webhdfs_url, data = file_data, method = 'PUT')
        #urlopen(req)  # actual write is done by redirect_request in HdfsHTTPRedirectHandler

# implement 'hdfs dfs -ls'
def hdfs_ls(args, user, webhdfs_prefix_url, default_dir):
    abs_path = args.path if args.path.startswith('/') else concat_path(default_dir, args.path)
    webhdfs_url = "{prefix_url}/{path}?user.name={user}&op=LISTSTATUS".format(
        prefix_url = webhdfs_prefix_url,
        path = abs_path.strip('/'),
        user = user
    )
    r = requests.get(webhdfs_url + '')
    r.raise_for_status()
    jsonData = r.json()

    print("Found {num_items} items".format(num_items = len(jsonData['FileStatuses']['FileStatus'])))
    for fs in jsonData['FileStatuses']['FileStatus']:
        print("{type}{permission} {num_replicas} {owner} {group} {size} {modificationTime} {path}".format(
            type = '-' if fs['type'] == 'FILE' else 'd',
            permission = get_str_perms(fs['permission']).ljust(11),
            num_replicas = fs['replication'] if fs['type'] == 'FILE' else '-',
            owner = fs['owner'],
            group = fs['group'],
            size = str(fs['length']).rjust(10),
            modificationTime = format_date(fs['modificationTime']),
            path = concat_path(args.path.rstrip('/'), fs['pathSuffix'])
        ))

# implement 'hdfs dfs -mkdir'
def hdfs_mkdir(args, user, webhdfs_prefix_url, default_dir):
    abs_path = args.path if args.path.startswith('/') else concat_path(default_dir, args.path)
    webhdfs_url = "{prefix_url}/{path}?user.name={user}&op=MKDIRS".format(
        prefix_url = webhdfs_prefix_url,
        path = abs_path.strip('/'),
        user = user
    )
    r = requests.put(webhdfs_url)
    r.raise_for_status()

# implement 'hdfs dfs -mv'
def hdfs_mv(args, user, webhdfs_prefix_url, default_dir):
    abs_src_path = args.src_path if args.src_path.startswith('/') else concat_path(default_dir, args.src_path)
    abs_dst_path = args.dst_path if args.dst_path.startswith('/') else concat_path(default_dir, args.dst_path)
    webhdfs_url = "{prefix_url}/{src}?user.name={user}&op=RENAME&destination={dst}".format(
        prefix_url = webhdfs_prefix_url,
        src = abs_src_path.lstrip('/'),
        user = user,
        dst = abs_dst_path
    )
    r = requests.put(webhdfs_url)
    r.raise_for_status()

# implement 'hdfs dfs -put'
def hdfs_put(args, user, webhdfs_prefix_url, default_dir):
    dst_path = args.path.pop()
    abs_dst_path = dst_path if dst_path.startswith('/') else concat_path(default_dir, dst_path)
    for src_path in args.path:
        dst_file = src_path.split('/').pop()
        webhdfs_url = "{prefix_url}/{path}?user.name={user}&op=CREATE&encoding=utf-8".format(
            prefix_url = webhdfs_prefix_url,
            path = concat_path(abs_dst_path.lstrip('/'), dst_file),
            user = user
        )
        file_data = open(src_path)
        req = Request(url = webhdfs_url, data = file_data, method = 'PUT')
        # actual write is done by redirect_request in HdfsHTTPRedirectHandler
        urlopen(req)

# implement 'hdfs dfs -rm'
def hdfs_rm(args, user, webhdfs_prefix_url, default_dir):
    for path in args.path:
        abs_path = path if path.startswith('/') else concat_path(default_dir, path)
        webhdfs_url = "{prefix_url}/{path}?user.name={user}&op=DELETE".format(
            prefix_url = webhdfs_prefix_url,
            path = abs_path.strip('/'),
            user = user
        )
        r = requests.delete(webhdfs_url)
        r.raise_for_status()

def load_config(conf_path) -> dict:
    with open(conf_path) as f:
        conf = yaml.safe_load(f)
    return conf

def main():
    # read configuration file
    conf = load_config('/home/cpreaud/dev/git/hdfs-cli/conf.yaml')
    nn_list = conf['NameNodes']

    # get active NameNode
    nn = get_active_nn(nn_list)

    user = getpass.getuser()
    default_dir = '/user/' + user
    webhdfs_prefix_url = "http://{nn}:{port}/{prefix}".format(
        nn = nn['Host'],
        port = nn['Port'],
        prefix = 'webhdfs/v1'
    )

    # install handlers for HTTP errors and HTTP redirects
    opener = build_opener(HdfsBaseHandler, HdfsHTTPRedirectHandler)
    install_opener(opener)

    parser = argparse.ArgumentParser(description = 'Perform operations on HDFS.')
    subparsers = parser.add_subparsers(help = 'list of HDFS operations')

    # cat subparser
    parser_cat = subparsers.add_parser(
        'cat',
        help = 'Fetch all files that match the file pattern <src> and display their content on stdout.'
    )
    parser_cat.add_argument(
        'path',
        metavar = '<path>',
        help = 'the file pattern to display'
    )
    parser_cat.set_defaults(func = hdfs_cat)

    # cp subparser
    parser_cp = subparsers.add_parser(
        'cp',
        help = 'Copy files that match the file pattern <src> to a destination.'
    )
    parser_cp.add_argument(
        'path',
        metavar = '<path>',
        nargs = '+',
        help = '<src> ... <dst>'
    )
    parser_cp.set_defaults(func = hdfs_cp)

    # ls subparser
    parser_ls = subparsers.add_parser(
        'ls',
        help = 'List the contents that match the specified file pattern.'
    )
    parser_ls.add_argument(
        'path',
        metavar = '<path>',
        nargs = '?',
        default = '',
        help = 'a path pattern, default to ' + default_dir
    )
    #parser_ls.add_argument(
    #    'default_dir',
    #    nargs = '?',
    #    default = default_dir,
    #    help = argparse.SUPPRESS
    #)
    parser_ls.set_defaults(func = hdfs_ls)

    # mkdir subparser
    parser_mkdir = subparsers.add_parser(
        'mkdir',
        help = 'Create a directory in specified location.'
    )
    parser_mkdir.add_argument(
        'path',
        metavar = '<path>',
        help = 'a directory path'
    )
    parser_mkdir.set_defaults(func = hdfs_mkdir)

    # mv subparser
    parser_mv = subparsers.add_parser(
        'mv',
        help = 'Rename a file or directory.'
    )
    parser_mv.add_argument(
        'src_path',
        metavar = '<src>',
        help = 'path of the source file/directory'
    )
    parser_mv.add_argument(
        'dst_path',
        metavar = '<dst>',
        help = 'path of the destination file/directory'
    )
    parser_mv.set_defaults(func = hdfs_mv)

    # put subparser
    parser_put = subparsers.add_parser(
        'put',
        help = 'Copy files from the local file system into HDFS.'
    )
    parser_put.add_argument(
        'path',
        metavar = '<path>',
        nargs = '+',
        help = '<local_src> ... <hdfs_dst>'
    )
    parser_put.set_defaults(func = hdfs_put)

    # rm subparser
    parser_rm = subparsers.add_parser(
        'rm',
        help = 'Delete all files that match the specified file pattern.'
    )
    parser_rm.add_argument(
        'path',
        metavar = '<path>',
        nargs = '+',
        help = 'a path pattern'
    )
    parser_rm.set_defaults(func = hdfs_rm)

    # parse and execute HDFS operation
    args = parser.parse_args()
    args.func(args, user, webhdfs_prefix_url, default_dir)

main()
