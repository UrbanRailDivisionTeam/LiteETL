# lite_etl

## 带有更新环境内网迁移不要忘记修改.venv/pyvenv.cfg文件下的python解释器路径

## 带有更新环境内网迁移不要忘记，对于oracle的驱动需要在手动复制符合版本的oci.dll等相关动态库文件到.venv/Script中

## 对于sql的编写，source和target的列名需相同

## 对于sql的编写，所有的第一列都必须是id且放在第一位

## 对于oracle数据库的id列，不能加双引号，否则生成的sql没办法跨异种数据库兼容