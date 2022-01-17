import pymysql
import os
from bs4 import BeautifulSoup
from lxml import etree
import shutil
import configparser
import sys

"""
@auther:一颗山竹屹立
@date:20211114
"""

# 获取日期
def get_reportdate(html_file):
    report_date = html_file[2:6]+'-'+html_file[6:8]+'-'+html_file[8:10]+' '+html_file[10:12]+':'+html_file[12:14]+':00'
    return report_date

# 获取需解析的最新的 jmeter 执行结果 html
# 除了文件名排序获取最新，还可以通过 get_ctime 获取最新日期的文件
def get_filenames(html_folder):
    files = os.listdir(html_folder)
    htmls = [html for html in files if '.html' in html]
    htmls.sort(reverse=True)
    html_file = htmls[0]
    return html_file

# 解析 html，获取:步骤名、samples、failures、success_Rate、average_Time(ms)、min_Time(ms)、max_Time(ms)
# 一次构建中获取失败的数据改为从数据库获取 
# 获取二次优化后的成功数据
def html_datas(html_folder,html_file):
    bs = BeautifulSoup(open(html_folder+'/'+html_file, 'r', encoding='utf8').read(), 'html.parser')
    success_case = [[cell.get_text() for cell in cells.find_all('td')[:-1]]
                    for cells in bs.find_all('tr', attrs={'valign': 'top', 'class': ''})]
    # failed_case = [[cell.get_text() for cell in cells.find_all('td')[:-1]]
    #                for cells in bs.find_all('tr', attrs={'valign': 'top', 'class': 'Failure'})[1:]]
    return [cell for cell in success_case if(cell and len(cell)==7)]

# 建立连接
def init_sql(sql_host,sql_user,sql_passwd,sql_db):
    connection = pymysql.connect(host=sql_host,
                        user=sql_user,
                        password=sql_passwd,
                        database=sql_db,
                        charset='utf8mb4',
                        # pymysql 获取到数据从默认的 tuple 转为 list
                        # cursorclass=pymysql.cursors.DictCursor
                        )
    return connection

# 发现每个自动化脚本对应的数据库第二个重要的字段名不一样
# 获取数据库第二个字段名
def get_key(table,sql_host,sql_user,sql_passwd,sql_db):
    db_con = init_sql(sql_host,sql_user,sql_passwd,sql_db)
    db = db_con.cursor()
    sql = "select * from {}".format(table)
    db.execute(sql)
    desc = db.description
    keys = []
    for field in desc:
        keys.append(field[0])
    db_con.close()
    return keys[1]

# 通过数据库获取一次构建失败的事务
def sql_datas(table,report_date,sql_host,sql_user,sql_passwd,sql_db,key):
    db_con = init_sql(sql_host,sql_user,sql_passwd,sql_db)
    print('连接数据库获取失败的事务...')
    db = db_con.cursor()
    sql = "select `{}` from {} where `report_date`='{}' and `success_Rate` = 0".format(key,table,report_date)
    db.execute(sql)
    fail_datas = db.fetchall()
    db_con.close()
    return [list(cell) for cell in fail_datas]

# 需要重新运行的事务
# 修改 jmx 文件中的事务
def jmx_modify(jmx_folder,fail_datas):
    jmx_tmp_folder = 'tmp'
    isExists=os.path.exists(jmx_folder + "/" + jmx_tmp_folder)
    if not isExists:
        os.makedirs(jmx_folder+'/'+jmx_tmp_folder) 
    files = os.listdir(jmx_folder)
    jmx_files = [file for file in files if '.jmx' in file]
    parser = etree.XMLParser(encoding='utf-8')
    for jmx_file in jmx_files:
        shutil.copyfile(jmx_folder+'/'+jmx_file, jmx_folder+'/'+jmx_tmp_folder+'/'+jmx_file)
        tree = etree.parse(jmx_folder+'/'+jmx_file,parser=parser)
        for ele1 in tree.iter(tag='TransactionController'):
            if ele1.get('enabled') == 'true':
                for ele2 in ele1.getnext().iter(tag='HTTPSamplerProxy'):
                    next_tree = ele2.getnext()
                    if ele1[1].text == 'true':
                        ele1.set("enabled","false")
                        if [ele1.get('testname')] in fail_datas or next_tree.find('RegexExtractor') is not None:
                            ele1.set("enabled","true")
                            break
                        else:pass
                    else: 
                        if [ele2.get('testname')] in fail_datas or next_tree.find('RegexExtractor') is not None:pass
                        else:ele2.set("enabled","false")
        document = open(jmx_folder+'/'+jmx_file, 'w',encoding="utf-8")
        document.write('<?xml version="1.0" encoding="UTF-8"?>\r')
        document.write(etree.tostring(tree,encoding='utf-8').decode('utf-8'))
        document.close()
        print(jmx_file+'脚本已处理完毕...')

# 运行 jmx 后新成功结果和旧的失败两个 data list 取交集，提取二次优化后的成功数据
def find_common(fail_datas,success_datas):
    update_datas = []
    for success_data in success_datas:
        if [success_data[0]] in fail_datas:
            update_datas.append(success_data)
    return update_datas

# 更新数据
def sql_update(table,report_date,update_datas,sql_host,sql_user,sql_passwd,sql_db,key):
    db_con = init_sql(sql_host,sql_user,sql_passwd,sql_db)
    db = db_con.cursor()
    print('正在更新数据库...')
    count = 0
    for data in update_datas:
        data[-1]=str(data[-1])[:-3]
        data[-2]=str(data[-2])[:-3]
        data[-3]=str(data[-3])[:-3]
        data[-4]=str(data[-4])[:-1]
        sql = "update {} set `samples`={}, `failures`={} , `success_Rate`={} , `average_Time(ms)`={} , `min_Time(ms)`={} , `max_Time(ms)`={} where `report_date`='{}' and `{}`='{}'".format(table,*data[1:],report_date,key,data[0])
        try:
            db.execute(sql)
            db_con.commit()
            count += 1
        except:
            db_con.rollback()
    db_con.close()
    print('数据库中更新了'+str(count)+'条数据')

def recover(jmx_folder):
    # 删除创建的新的 jmx 文件
    files = os.listdir(jmx_folder)
    jmx_files = [file for file in files if '.jmx' in file]
    for jmx_file in jmx_files:
        os.remove(jmx_folder+'/'+jmx_file)
    # 将 tmp 中的原始文件移到最开始的位置
    jmx_tmp_folder = 'tmp'
    files = os.listdir(jmx_folder + "/" + jmx_tmp_folder)
    jmx_files = [file for file in files if '.jmx' in file]
    for jmx_file in jmx_files:
        shutil.copyfile(jmx_folder+'/'+jmx_tmp_folder+'/'+jmx_file,jmx_folder+'/'+jmx_file)
    # 删除 tmp 文件夹
    shutil.rmtree(jmx_folder+'/'+jmx_tmp_folder)

# 解析配置文件
def parse_config(config_file):
    dirname, filename = os.path.split(os.path.abspath(__file__)) 
    config = configparser.ConfigParser()
    # config.read(dirname+'/'+config_file,encoding="utf-8")
    config.read(dirname+'/reconfig'+'/'+config_file,encoding="utf-8")
    output = {}
    for section in config.sections():
        output[section] = {}
        for key in config[section]:
            val_str = str(config[section][key])
            output[section][key] = val_str
    return output

# 判断数据库中是否存在对应日期的数据
def is_exist(report_date,sql_host,sql_user,sql_passwd,sql_db,table):
    db_con = init_sql(sql_host,sql_user,sql_passwd,sql_db)
    db = db_con.cursor()
    sql = "select * from {} where `report_date`='{}'".format(table,report_date)
    db.execute(sql)
    results = db.fetchall()
    db_con.close()
    if len(results) != 0:
        return True
    else:
        return False
            
if __name__ == "__main__":
    # config_file = 'config.ini'
    config_file = sys.argv[1]
    config = parse_config(config_file)

    html_folder = config['html']['html_folder']
    table = config['db']['table']
    jmx_folder = config['jmx']['jmx_folder']
    sql_host = config['db']['host']
    sql_user = config['db']['user']
    sql_passwd = config['db']['password']
    sql_db = config['db']['database']
    cmd = config['jmx']['jmx_exc']
    # 二次优化之前的结果
    try:report_date = sys.argv[2]
    except:report_date = get_reportdate(get_filenames(html_folder))
    if is_exist(report_date,sql_host,sql_user,sql_passwd,sql_db,table):
        key = get_key(table,sql_host,sql_user,sql_passwd,sql_db)
        fail_datas = sql_datas(table,report_date,sql_host,sql_user,sql_passwd,sql_db,key)
        jmx_modify(jmx_folder,fail_datas)
        os.system(cmd)
        success_html = get_filenames(html_folder)
        success_datas = html_datas(html_folder,success_html)
        update_datas = find_common(fail_datas,success_datas)
        sql_update(table,report_date,update_datas,sql_host,sql_user,sql_passwd,sql_db,key)
        recover(jmx_folder)
    else:
        print('数据库中不存在对应日期的数据结果，请确认 html 结果文件：1.html 结果文件夹中删除最新文件直至想要重构的结果数据成为最新；2.或者命令加上第二个日期参数')