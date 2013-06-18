# -*- coding: utf-8 -*-

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table,Column, Integer, String,Sequence,Boolean,DateTime
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

engine = create_engine('mysql://root:123@localhost/weibodata?charset=utf8',echo=False)
# This custom-made Session class will create new Session objects
# which are bound to our database.
Session = sessionmaker()
# instantiate a Session: talk to database
Session.configure(bind=engine)  # once engine is available
session = Session()

Base = declarative_base()
metadata = Base.metadata

'''
# engine = create_engine('sqlite:///:memory:', echo=False)
engine = create_engine('mysql://root:123@localhost/weibodata?charset=utf8',echo=False)
'''

class NearByInfo(Base):
    __tablename__ = 'nearbyinfo'
    id = Column(Integer,Sequence('weibo_id_seq'),primary_key=True)
    text = Column(String(240),nullable=False)
    created_at = Column(DateTime,nullable=False)
    distance = Column(String(45),nullable=False)
    mid = Column(String(45),nullable=False)
    user_id = Column(String(45),nullable=False)
    user_city = Column(Integer,nullable=False)
    user_followers_count = Column(Integer,nullable=False)
    user_location = Column(String(45),nullable=False)
    user_type = Column(Integer,nullable=False)
    user_profile_url = Column(String(45),nullable=False)
    user_province = Column(String(45),nullable=False)
    user_description = Column(String(145),nullable=False)
    user_statuses_count = Column(Integer,nullable=False)
    user_created_at = Column(DateTime,nullable=False)
    user_gender = Column(String(2),nullable=False)
    # user_geo_enable = Column(Boolean,nullable=False)
    user_name = Column(String(45),nullable=False)
    place_lat = Column(DOUBLE,nullable=False)
    place_lon = Column(DOUBLE,nullable=False)
    place_title = Column(String(45),nullable=False)
    place_poiid = Column(String(45),nullable=False)
    place_type = Column(String(45),nullable=False)

    """docstring for NearByInfo"""
    def __init__(self,  text="",
                        created_at = "",
                        distance= "",
                        mid="",
                        user_id="",
                        user_city=0,
                        user_followers_count=0,
                        user_location="",
                        user_type = 0,
                        user_profile_url = "",
                        user_province = "",
                        user_description = "",
                        user_statuses_count=0,
                        user_created_at = "",
                        user_gender ="",
                        # user_geo_enable = True,
                        user_name = "",
                        place_lat = 0,
                        place_lon = 0,
                        place_title = "",
                        place_poiid = "",
                        place_type = "checkins",
                        ):

        super(NearByInfo, self).__init__()
        self.text = text
        self.created_at = created_at
        self.distance = distance
        self.mid = mid
        self.user_id = user_id
        self.user_city = user_city
        self.user_followers_count = user_followers_count
        self.user_location = user_location
        self.user_type = user_type
        self.user_profile_url = user_profile_url
        self.user_province = user_province
        self.user_description = user_description
        self.user_statuses_count = user_statuses_count
        self.user_created_at = user_created_at
        self.user_gender = user_gender
        self.user_geo_enable = user_geo_enable
        self.user_name = user_name
        self.place_lat = place_lat
        self.place_lon = place_lon
        self.place_title = place_title
        self.place_poiid = place_poiid
        self.place_type = place_type

    def __init__(self, arg):
        super(NearByInfo, self).__init__()
        self.text = arg[0]
        self.created_at = arg[1]
        self.distance = arg[2]
        self.mid = arg[3]
        self.user_id = arg[4]
        self.user_city = arg[5]
        self.user_followers_count = arg[6]
        self.user_location = arg[7]
        self.user_type = arg[8]
        self.user_profile_url = arg[9]
        self.user_province = arg[10]
        self.user_description = arg[11]
        self.user_statuses_count = arg[12]
        self.user_created_at = arg[13]
        self.user_gender = arg[14]
        self.user_geo_enable = arg[15]
        self.user_name = arg[16]
        self.place_lat = arg[17]
        self.place_lon = arg[18]
        self.place_title = arg[19]
        self.place_poiid = arg[20]
        self.place_type = arg[21]

    def __repr__(self):
        return "<NearByInfo('%s','%s', '%s','%s','%s')>" % (
                self.mid, 
                self.user_id, 
                self.place_title,
                str(self.place_lat),
                str(self.place_lon),
                )

'''        
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    name = Column(String(50))
    fullname = Column(String(50))
    password = Column(String(12))

    def __init__(self, name, fullname, password):
        self.name = name
        self.fullname = fullname
        self.password = password

    def __repr__(self):
        return "<User('%s','%s', '%s')>" % (self.name, self.fullname, self.password)
'''

metadata.create_all(engine) 

'''
ed_user = User('ed', 'Ed Jones', 'edspassword')
print ed_user.name,ed_user.password,str(ed_user.id)

from sqlalchemy.orm import sessionmaker
# This custom-made Session class will create new Session objects
# which are bound to our database.
Session = sessionmaker(bind=engine)

# instantiate a Session: talk to database
session = Session()
session.add(ed_user)    # ed_user 还是pending状态，没有写入到数据库

our_user = session.query(User).filter_by(name='ed').first() 
print our_user
ed_user is our_user

usrs = [User('wendy', 'Wendy Williams', 'foobar'),
        User('mary', 'Mary Contrary', 'xxg527'),
        User('fred', 'Fred Flinstone', 'blah')]
session.add_all(usrs)
del usrs
print session.dirty
ed_user.password = 'f8s7ccs'
print session.dirty
print session.new  
# issue all remaining changes to the database and commit the transaction
session.commit()
ed_user.name = 'Edwardo'
print session.dirty
fake_user = User('fakeuser', 'Invalid', '12345')
session.add(fake_user)
print session.new
print fake_user in session
print session.query(User).filter(User.name.in_(['Edwardo', 'fakeuser'])).all() 
session.rollback()
print ed_user.name
print fake_user in session # 在回滚后 fake_user就不在 session 中了

print session.query(User).filter(User.name.in_(['ed', 'fakeuser'])).all() 
'''

