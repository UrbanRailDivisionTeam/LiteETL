import sqlalchemy


connect_str = "mysql://cheakf:Swq8855830.@localhost:9004/default"
coonnect = sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=6000, pool_recycle=3600)
with coonnect.connect() as conn:
    res = conn.execute(sqlalchemy.text("select 1")).fetchall()[0]
    print(res)
    
    
