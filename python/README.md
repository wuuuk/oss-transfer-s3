# ossTransferToS3
将oss存储桶下的所有资源迁移至s3，python编写

## 配置config.yaml文件
```bash
oss:
  endpoint: oss-ap-southeast-1.aliyuncs.com  # oss地域节点
  accesskeyid: xxxxxx  # 阿里云access key id
  accesskeysecret: xxxxxx  # 阿里云access key secret
  bucketname: xxxxxx  # bucket(桶)名称

s3:
  endpoint: ap-southeast-1  # s3区域
  accesskeyid: xxxxxx  # aws access key id
  accesskeysecret: xxxxxx  # aws access key secret
  bucketname: xxxxxx  # s3(桶)名称
  token: ""
```
+ ⚠️⚠️如果桶的位置和服务器所在区域是同一个，则可使用内网地址 "xxx-internal.aliyuncs.com"
+ ⚠️⚠️⚠️⚠️ 最好运行在海外服务器节点上，海外服务器节点连接aws s3比较快。大陆访问s3节点稍微比较慢

- ##### 运行：
```
python3 -m pip install -r requirements.txt  # 安装依赖
python3 ossTransferToS3.py
```

## 其他语言: 
+ go: https://github.com/wuuuk/oss-transfer-s3/tree/main/go
+ java: https://github.com/wuuuk/oss-transfer-s3/tree/main/java
+ node.js: https://github.com/wuuuk/oss-transfer-s3/tree/main/nodejs
