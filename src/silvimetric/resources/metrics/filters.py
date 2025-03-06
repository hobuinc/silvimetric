def f_2plus(data):
    return data[data['HeightAboveGround'] > 2]

def make_hag_filter(val):
    def f_hag_gt_val(data):
        return data[data['HeightAboveGround'] > val]
    return f_hag_gt_val

def make_z_filter(val):
    def f_z_gt_val(data):
        return data[data['Z'] > val]
    return f_z_gt_val