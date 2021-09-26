# -*- coding: utf-8 -*- 
import os
import re
import sys
import json
import collections

import delegator

import DatabaseUtil

# 配置信息
configDic = {}

CommitFile = collections.namedtuple('CommitFile', ['status', 'path', 'filename', 'ext'])
MetaFile = collections.namedtuple('MetaFile', ['path', 'guid'])

class CommitInfo:
    """ svn 提交信息
    
    用于获取和解析 svn 提交信息
    """

    def __init__(self, repos, txn):
        self.repos = repos
        self.txn = txn

        command = delegator.chain(
            command=f'env | svnlook changed {self.repos} -t {self.txn}', 
            env={'LANG': 'en_US.UTF-8', 'LC_ALL': 'en_US.UTF-8'})

        regex = r'([A|D|U])\s+((\w+\/?(?:(?:\.\w+)*(?=\.))*)+(\.\w+)*)'
        match = re.findall(regex, command.out)
        print("command.out:")
        print(command.out)

        self._commitFiles = [CommitFile(*file) for file in match]
        self._commitAssets = []
        self._commitMetas = []

        self.deleteMeta = []
        self.updateMeta = []
        self.addMeta = []
        self.repeatMeta = collections.defaultdict(list)
    
    def get_commit_assets(self):
        """ 获取提交信息中的资源文件

        资源文件区别于 .meta 文件和目录
        """
        if self._commitAssets:
            return self._commitAssets
        
        for commit in commitFiles:
            if commit.ext and commit.ext != '.meta':
                self._commitAssets.append(commit)
        return self._commitAssets
    
    def get_commit_mates(self):
        """ 获取提交信息中的 .meta 文件
        """
        if self._commitMetas:
            return self._commitMetas
        
        for commit in commitFiles:
            if commit.ext and commit.ext == '.meta':
                self._commitMetas.append(commit)
        return self._commitMetas

    def parse_meta(self):
        """ 解析提交信息中的 .meta 文件
        """
        def get_guid(path):
            """ 获取指定文件中的 guid
            """
            command = delegator.chain(
                command=f'env | svnlook cat -t {self.txn} {self.repos} {path}', 
                env={'LANG': 'en_US.UTF-8', 'LC_ALL': 'en_US.UTF-8'})

            regex = r'guid:\s*(\w+)'
            match = re.search(regex, command.out)

            return match.group(1)

        guids = []
        for commit in self.get_commit_mates():
            if commit.status == 'D':
                self.deleteMeta.append(MetaFile(commit.path, ''))
            else:
                guid = get_guid(commit.path)

                if guid in guids:   # 重复的GUID
                    self.repeatMeta[guid].append(MetaFile(commit.path, guid))
                guids.append(guid)

                if commit.status == 'U':
                    self.updateMeta.append(MetaFile(commit.path, guid))
                if commit.status == 'A':
                    self.addMeta.append(MetaFile(commit.path, guid))

    def exist(self, filePath, fileType=None, status=None):
        """ 查找指定文件路径是否存在
        
        param status: 文件状态
        param type: 文件类型 meta/asset/directory
        """
        files = self._commitFiles
        if fileType:
            if fileType == 'meta':
                files = self.get_commit_mates
            if fileType == 'asset':
                files = self.get_commit_assets
        
        if status:
            files = [file for file in files if file.status in status]
        
        for file in files:
            if file.path == filePath:
                return True
        
        return False

def check_commit_file_guid(commitInfo):
    """ 检查提交文件 GUID
    
    param commitInfo: svn 提交信息
    """

    # .meta同步处理检查 & GUID 检查
    return check_meta_sync(commitInfo) & check_guid(commitInfo)

def check_meta_sync(commitInfo):
    """ .meta同步处理检查
    """
    notDeleteMeta = []
    notAddMeta = []

    for file in commitInfo.get_commit_assets():
        metaFile = file.path.join('.meta')

        # 检查被删除文件，是否仍然存在 .meta 文件
        if file.status == 'D':
            if not commitInfo.exist(metaFile, fileType='meta', status=('D')):
                isExist = commitInfo.exist(metaFile, fileType='meta', status=('A')) or DatabaseUtil.exist_by_path(metaFile)
                if isExist:
                    notDeleteMeta.append(file)

        # 检查新增文件，是否存在 .meta 文件
        if file.status == 'A':
            if not commitInfo.exist(metaFile, fileType='meta', status=('A', 'U')):
                isExist = not commitInfo.exist(metaFile, fileType='meta', status=('D')) and DatabaseUtil.exist_by_path(metaFile)
                if not isExist:
                    notAddMeta.append(file)
    
    # 输出未同步信息
    if notDeleteMeta or notAddMeta:
        os.system('echo "[文件提交需与相应.meta文件同步处理]" >&2')
        os.system(f'echo "存在未同步处理的文件有{ len(notDeleteMeta) + len(notAddMeta) }个：" >&2')
        # TODO 配置表控制输出数量
        for file in notAddMeta:
            metaFilePath = file.path + '.meta'
            os.system(f'echo "上传文件：{file.path} 相应的.meta文件未同步处理">&2')
            os.system(f'echo "需要上传相应的.meta文件：{metaFilePath}">&2')
        for file in notDeleteMeta:
            metaFilePath = file.path + '.meta'
            os.system(f'echo "删除文件：{file.path} 相应的.meta文件未同步处理">&2')
            os.system(f'echo "需要删除相应的.meta文件：{metaFilePath}">&2')
        
        return False

    return True

def check_guid(commitInfo):
    """ TODO GUID 检查
    """
    commitInfo.parse_meta()

    repeatMeta = commitInfo.repeatMeta  # key:重复的guid  value:重复的 .meta 文件
    modifyMeta = {}                     # key:源guid  value:被修改的 .meta 文件

    # 检查新增的 .meta 文件，是否与现有的 guid 重复
    for meta in commitInfo.addMeta:
        res = DatabaseUtil.select_by_guid(meta.guid)
        metaFiles = [meta for meta in res if meta not in commitInfo.deleteMeta]

        if metaFiles:
            if not meta in repeatMeta[meta.guid]:
                repeatMeta[meta.guid].append(meta)
            repeatMeta[meta.guid] += metaFiles
            
    # 检查修改的 .meta 文件，guid 是否变化
    for meta in commitInfo.updateMeta:
        res = DatabaseUtil.select_by_path(meta.path)
        if meta.guid != res.guid:
            modifyMeta[res.guid] = meta

    isPass = True
    # 输出存在问题的 guid 文件信息
    if modifyMeta:
        os.system('echo "[上传文件存在更改guid的文件]" >&2')
        os.system(f'echo "更改guid的文件有{len(modifyMeta.keys())}个：" >&2')
        for guid, meta in modifyMeta.items():
            os.system(f'echo "上传文件：{meta.path} 的guid: {meta.guid} 存在更改">&2')
            os.system(f'echo "文件原guid为:{guid} ">&2')
        isPass = False
    if repeatMeta:
        os.system('echo "[上传文件存在已存在guid]" >&2')
        os.system(f'echo "重复的guid有{len(repeatMeta.keys())}个：" >&2')
        for guid, metas in repeatMeta.items():
            os.system(f'echo "guid: {guid} 存在重复" >&2')
            os.system(f'echo "guid 重复的文件：" >&2')
            for meta in metas:
                os.system(f'echo "文件：{meta.path} 的guid: {meta.guid} 重复了" >&2')
        isPass = False

    return isPass


def get_config(configPath=None):
    """ 获取配置文件

    如果找不到配置文件，则默认不进行检查

    默认配置文件路径：./config/SVNToolSetting.json
    测试配置文件路径：./config/SVNToolSetting.test.json
    """
    global configDic

    configPath = configDic if configDic else './config/SVNToolSetting.json'
    if os.path.exists(configPath):
        with open(configPath, 'r') as configFile:
            configDir = json.load(configFile)

    return configDic

def main():
    global configDic

    repos = sys.argv[1]     # 版本库路径
    txn = sys.argv[2]       # 提交事务名称
    print(f'repos={repos} txn={txn}')

    configDic = get_config()
    if not configDic.get('enable'):
        return 0
    
    isPass = True
    try:
        DatabaseUtil.init_database(configDic)

        commitInfo = CommitInfo(repos, txn)
        isPass = check_commit_file_guid(commitInfo)
    except Exception as ex:
        pass

    return isPass

if __name__ == '__main__':
    main()
