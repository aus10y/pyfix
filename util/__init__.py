import os
import pickle

path = str(os.path.abspath(os.path.dirname(__file__)))

#------------------------------------------------------------------------------

SOH = b'\x01'  # 'Start of Heading', follows every message field

# FIX tags are keys, messages are values
with open(path+'/FIXtags.pkl', 'rb') as f:
    TAGS = pickle.load(f)

# FIX messages are keys, tags are values
with open(path+'/FIXmsgs.pkl', 'rb') as f:
    MSGS = pickle.load(f)

#------------------------------------------------------------------------------