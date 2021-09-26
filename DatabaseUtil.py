# -*- coding: utf-8 -*- 
import records

from PreCommit import MetaFile


db = None
dbUrl = ''

def init_database(config):
    """ 连接数据库
    """
    global db
    global dbUrl

    dbUrl = config['database']
    db = records.Database(dbUrl)

def exist_by_path(filePath):
    sql = f'SELECT count(*) FROM `mecha_hooks_meta_guid` WHERE `path` = {filePath}'
    print(sql)

    res = db.query(sql)
    return bool(rows[0]['count(*)'])

def select_by_guid(guid):
    sql = f'SELECT `path`, `guid` FROM `mecha_hooks_meta_guid` WHERE `guid` = {guid}'
    print(sql)

    res = db.query(sql)
    metaFiles = [MetaFile(meta.path, meta.guid) for meta in res]
    return metaFiles

def select_by_path(filePath):
    sql = f'SELECT `path`, `guid` FROM `mecha_hooks_meta_guid` WHERE `path` = {filePath} LIMIT 1'
    print(sql)

    res = db.query(sql)
    return MetaFile(res[0].path, res[0].guid)