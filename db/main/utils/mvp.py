import requests

'''
This function returns a list of BGP vantage points strategically positioned
to retrieve useful data thanks to MVP.
'''
# def get_vps(nb_vps):
#     # Get list of VPs from MVP (temporary URL).
#     data = requests.get(url="http://5.161.124.63/mvp?v={}GB&y=2022".format(1000)).json()
    
#     vps = set()
#     for vpname in data['VP_set']:
#         # Return tuple collector,peer_asn
#         vps.add((vpname.split('_')[0], int(vpname.split('_')[1])))
        
#         # Stop when nb_vps have been selected.
#         if len(vps) == nb_vps:
#             break

#     return vps

def get_vps(nb_vps):
    vps = set()

    with open('utils/mvp.txt', 'r') as fd:
        for line in fd.readlines():
            if len(vps) == nb_vps:
                return vps
            
            line = line.rstrip('\n')
            vps.add((line.split('_')[0], int(line.split('_')[1])))

    return vps


if __name__ == "__main__":
    print (get_vps(200))

