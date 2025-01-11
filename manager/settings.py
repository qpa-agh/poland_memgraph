CLEAR_PREPROCESSED = [False]

def toggle_clear_preprocessed():
    global CLEAR_PREPROCESSED
    CLEAR_PREPROCESSED[0] = not CLEAR_PREPROCESSED[0]
    if CLEAR_PREPROCESSED[0]:
        print('Clearing preprocessed data is on')
    else:
        print('Clearing preprocessed data is off')
        
def get_clear_preprocessed_value():
    return CLEAR_PREPROCESSED[0]