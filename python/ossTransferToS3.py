#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
from json import load
import oss2
import yaml
import boto3
import time

class OssTransferToS3:
    def __init__(self) -> None:
        pass

    def _ReadConfig(self, config=None):
        # 读取配置文件
        fileName = "./config.yaml"
        if config is None:
            with open(fileName, encoding='utf-8') as _cFile:
                try:
                    config = yaml.load(_cFile, Loader=yaml.FullLoader)
                except AttributeError as e:
                    config = yaml.load(_cFile, Loader=yaml.Loader)
                print("read config from", fileName)
        return config

    def _InitOss(self, config) -> oss2.Bucket:
        # 初始化oss
        try:
            _endpoint = "http://" + config.get("endpoint") if not config.get("endpoint").startswith("http://") else config.get("endpoint")
            client = oss2.Auth(config.get("accesskeyid"), config.get("accesskeysecret"))
            bucket = oss2.Bucket(client, _endpoint, config.get("bucketname"))
            print("init oss bucket success., region: {}".format(config.get("endpoint")))
            return bucket
        except BaseException as e:
            print("init oss error, msg: {}".format(e))

    def _InitS3(self, config):
        # 初始化S3
        try:
            bucket = boto3.client(
                service_name = "s3",
                region_name = config.get("endpoint"),
                aws_access_key_id = config.get("accesskeyid"),
                aws_secret_access_key = config.get("accesskeysecret"),
                aws_session_token = "",
            )
            print("init s3 bucket success., region: {}".format(config.get("endpoint")))
            return bucket
        except BaseException as e:
            print("init s3 error, msg: {}".format(e))

    def OssGetByteObject(self, bucket: oss2.Bucket, fileName):
        # oss流式下载
        try:
            dataStream = bucket.get_object(fileName)
            # dataStream.close()  # 不能关闭流，关闭之后则变成空的数据流
            return dataStream
        except BaseException as e:
            print("oss get object error, file name: {}, msg: {}".format(fileName, e))
            return False

    def S3UploadObject(self, bucket_client, bucketName, fileStream, fileName):
        # s3上传
        try:
            resp = bucket_client.upload_fileobj(
                fileStream.stream, 
                bucketName, 
                fileName, 
                ExtraArgs={
                    'ACL': 'public-read'
                },
                )
            return True if resp is None else False
        except BaseException as e:
            print("upload file error, msg: {}".format(e))
            return False

    def formatFileSize(self, fileSize):
        if fileSize < 1024:
            return "{}B".format(fileSize)
        elif fileSize < 1024 * 1024:
            return "{}KB".format(round(fileSize / 1024, 2))
        elif fileSize < 1024 * 1024 * 1024:
            return "{}MB".format(round(fileSize / 1024 / 1024, 2))
        elif fileSize < 1024 * 1024 * 1024 * 1024:
            return "{}GB".format(round(fileSize / 1024 / 1024 / 1024, 2))
        elif fileSize < 1024 * 1024 * 1024 * 1024 * 1024:
            return "{}TB".format(round(fileSize / 1024 / 1024 / 1024/ 1024, 2))

    def main(self):
        config = self._ReadConfig()
        ossBucket = self._InitOss(config.get("oss"))
        s3Bucket = self._InitS3(config.get("s3"))
        bkList = oss2.ObjectIterator(ossBucket)  # 列出所有文件
        totalCount = 0
        for i in bkList:
            fail_count = 0
            startTime = time.time()
            print("fileName: {}, size: {}, ".format(i.key, self.formatFileSize(i.size)), end='')
            while fail_count < 3:
                buffer = self.OssGetByteObject(ossBucket, i.key)
                if not buffer:
                    fail_count += 1
                    continue
                print("download success --> useTime {}s, ".format(round(time.time() - startTime, 2)), end='')
                _secondTime = time.time()
                res = self.S3UploadObject(s3Bucket, config.get("s3").get("bucketname"), buffer, i.key)
                if not res:
                    fail_count += 1
                    continue
                print("upload success --> useTime: {}s, total time: {}s".format(round(time.time() - _secondTime, 2), round(time.time() - startTime, 2)))
                totalCount += 1
                break
            break
            if totalCount % 1000 == 0:
                print("current count: {}".format(totalCount))
        print("All files copied, total count: ", totalCount)

if __name__ == "__main__":
    ott = OssTransferToS3()
    ott.main()
