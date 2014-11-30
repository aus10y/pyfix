import pickle

#------------------------------------------------------------------------------

SOH = b'\x01'  # 'Start of Heading', follows every message field

# FIX tags are keys, messages are values
with open('FIXtags.pkl', 'rb') as f:
    TAGS = pickle.load(f)

# FIX messages are keys, tags are values
with open('FIXmsgs.pkl', 'rb') as f:
    MSGS = pickle.load(f)

#------------------------------------------------------------------------------