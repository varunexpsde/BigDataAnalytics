Important chunk 1:    

    num_rec_ar = get_num_rec(nodes_rdd[0], label_col, bins, f_num, len(labels))

    num_rec_frac_ar, current_node_imp, Infogain_ar = get_Infogain_ar(num_rec_ar)

#     print(Infogain_ar.shape)

#     print(Infogain_ar[0])

#     print(current_node_imp)

#     print(num_rec_frac_ar.shape)

#     print(num_rec_frac_ar[0])

#     print(num_rec_ar[0])

    dec_ind = np.where(Infogain_ar==Infogain_ar.max())

    dec_ind = list(a_i[0] for a_i in dec_ind)

#     print(dec_ind)

    tree_dic[0] = (dec_ind[0], (dec_ind[1]+1)*bin_width[str(dec_ind[0])])

    rec_t_dec = (dec_ind[0], dec_ind[1])

    nodes_imp[0] = current_node_imp

    nodes_rec_frac[0] = num_rec_ar[0,bins-1,0,:]

    nodes_rec_frac[1] = num_rec_ar[rec_t_dec[0], rec_t_dec[1],0,:]

    nodes_rec_frac[1] = num_rec_ar[rec_t_dec[0], rec_t_dec[1],1,:]

#     print(rec_t_dec)

#     print(nodes_rec_frac)

#     print(nodes_imp)

    l_rdd , r_rdd = get_lr_rdd(nodes_rdd[0], rec_t_dec)

    nodes_rdd[1] = l_rdd

    nodes_rdd[2] = r_rdd

    num_rec_ar = get_num_rec(nodes_rdd[1], label_col, bins, f_num, len(labels))

    num_rec_frac_ar, current_node_imp, Infogain_ar = get_Infogain_ar(num_rec_ar)

    print(Infogain_ar.shape)

    print(Infogain_ar[0])

    print(current_node_imp)

    print(num_rec_frac_ar.shape)

    print(num_rec_frac_ar[0])

    print(num_rec_ar[0])

    print(np.nanmax(Infogain_ar))
    
    print(np.where(Infogain_ar==np.nanmax(Infogain_ar)))
#     dec_ind = np.where(Infogain_ar==Infogain_ar.max())

#     dec_ind = list(a_i[0] for a_i in dec_ind)


Important Chunk 2:

    #i = 0
    
    #while max(tree_dic, key=tree_dic.get)==(2**(max_depth)-1):

    for i in range(0,3): 
    
        num_rec_ar = get_num_rec(nodes_rdd[i], label_col, bins, f_num, len(labels))

        num_rec_frac_ar, current_node_imp, Infogain_ar = get_Infogain_ar(num_rec_ar)

    #     print(Infogain_ar.shape)

    #     print(Infogain_ar[0])

    #     print(current_node_imp)

    #     print(num_rec_frac_ar.shape)

    #     print(num_rec_frac_ar[0])

    #     print(num_rec_ar[0])
    
        print(Infogain_ar.shape)

        dec_ind = np.where(Infogain_ar==Infogain_ar.max())
        
        print(Infogain_ar.max())
        
        print(dec_ind)

        dec_ind = list(a_i[0] for a_i in dec_ind)

    #     print(dec_ind)

        tree_dic[i] = (dec_ind[0], (dec_ind[1]+1)*bin_width[str(dec_ind[0])])

        rec_t_dec = (dec_ind[0], dec_ind[1])

        nodes_imp[i] = current_node_imp

        nodes_rec_frac[i] = num_rec_ar[0,bins-1,0,:]

        nodes_rec_frac[i+1] = num_rec_ar[rec_t_dec[0], rec_t_dec[1],0,:]

        nodes_rec_frac[i+2] = num_rec_ar[rec_t_dec[0], rec_t_dec[1],1,:]

    #     print(rec_t_dec)

    #     print(nodes_rec_frac)

    #     print(nodes_imp)

        l_rdd , r_rdd = get_lr_rdd(nodes_rdd[i], rec_t_dec)

        nodes_rdd[i+1] = l_rdd

        nodes_rdd[i+2] = r_rdd
        
        print(tree_dic)
        
        #i+=1
		
		
Important Chunk 3:

{0: (22, 62.746874999999996), 1: (27, 0.1455), 2: (1, 5.5443750000000005), 3: (22, 56.4721875), 4: (21, 11.724999999999998), 5: (7, 0.0691625), 6: (20, 7.9059375)}
{0: [288, 173], 1: [280, 28], 2: [8, 145], 3: [272, 12], 4: [8, 16], 5: [6, 4], 6: [2, 141], 7: [261, 6], 8: [11, 6], 9: [6, 1], 10: [2, 15], 11: [6, 0], 12: [0, 4], 13: [1, 0], 14: [1, 141]}