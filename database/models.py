import database_config as db_config
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql.expression import text
from sqlalchemy.sql.sqltypes import TIMESTAMP



class Post(db_config.Base):
   __tablename__ = "coins_table"
   

    
class User(db_config.Base):
    pass


class Vote(db_config.Base):
    pass

