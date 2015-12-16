# -*- coding: utf-8 -*-

"""
设计db模块的原因：
  1. 更简单的操作数据库
      一次数据访问：   数据库连接 => 游标对象 => 执行SQL => 处理异常 => 清理资源。
      db模块对这些过程进行封装，使得用户仅需关注SQL执行。
  2. 数据安全
      用户请求以多线程处理时，为了避免多线程下的数据共享引起的数据混乱，
      需要将数据连接以ThreadLocal对象传入。
设计db接口：
  1.设计原则：
      根据上层调用者设计简单易用的API接口
  2. 调用接口
      1. 初始化数据库连接信息
          create_engine封装了如下功能:
              1. 为数据库连接 准备需要的配置信息
              2. 创建数据库连接(由生成的全局对象engine的 connect方法提供)
          from transwarp import db
          db.create_engine(user='root',
                           password='password',
                           database='test',
                           host='127.0.0.1',
                           port=3306)
      2. 执行SQL DML
          select 函数封装了如下功能:
              1.支持一个数据库连接里执行多个SQL语句
              2.支持链接的自动获取和释放
          使用样例:
              users = db.select('select * from user')
              # users =>
              # [
              #     { "id": 1, "name": "Michael"},
              #     { "id": 2, "name": "Bob"},
              #     { "id": 3, "name": "Adam"}
              # ]
      3. 支持事物
         transaction 函数封装了如下功能:
             1. 事务也可以嵌套，内层事务会自动合并到外层事务中，这种事务模型足够满足99%的需求
"""
