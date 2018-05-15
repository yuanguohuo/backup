#!/bin/bash

dir=`dirname $0`
if [[ -f $dir/s3cfg ]] ; then
. $dir/s3cfg
fi

mnt=
uaccid=
useckey=
userid=

function usage()
{
    echo "Usage: goofys_test.sh -m {mount-point} [-i {user-access-id} -k {user-secret-key} -u {uid}]"
}

while getopts "m:i:k:u:h" opt ; do
    case $opt in
        m)
            mnt=$OPTARG
            ;;
        i)
            uaccid=$OPTARG
            ;;
        k)
            useckey=$OPTARG
            ;;
        u)
            userid=$OPTARG
            ;;
        h)
            usage
            exit 0
            ;;
        *)
            echo "Error: unrecognized option $opt"
            usage
            exit 1
            ;;
    esac
done

if [[ -z "$mnt" ]] ; then
    echo "Error: argument mount-point is missing"
    usage
    exit 1
fi

if [[ -z "$uaccid" ]] || [[ -z "$useckey" ]] || [[ -z "$userid" ]] ; then
    uaccid=$token
    useckey=$secret
    userid=$uid
fi

if [[ -z "$uaccid" ]] || [[ -z "$useckey" ]] || [[ -z "$userid" ]] ; then
    echo "Error: user-access-id, user-secret-key or uid is missing, and there is no default value"
    usage
    exit 1
fi


RootDir=$mnt
UsrDir=$RootDir/${uaccid}_${userid}
timestamp=`date +%Y%m%d%H%M%S`

function compare_files()
{
    local src_file=$1
    local dst_file=$2
    local verbose=$3

    if [ ! -f $src_file ] ; then
        echo "ERROR: compare_files: param1 $src_file doesn't exist or isn't a file"
        return 1
    fi

    if [ ! -f $dst_file ] ; then
        echo "ERROR: compare_files: param2 $dst_file doesn't exist or isn't a file"
        return 1
    fi

    local src_size=`ls -a -l $src_file | tr -s ' ' | cut -d ' ' -f 5`
    local dst_size=`ls -a -l $dst_file | tr -s ' ' | cut -d ' ' -f 5`

    local src_md5=`md5sum $src_file | tr -s ' ' | cut -d ' ' -f 1`
    local dst_md5=`md5sum $dst_file | tr -s ' ' | cut -d ' ' -f 1`

    if [ $src_size -ne $dst_size ] ; then
        echo "ERROR: compare_files: size of $src_file ($src_size) doesn't match with size of $dst_file ($dst_size)"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: compare_files: size of $src_file ($src_size) matches with size of $dst_file ($dst_size)"
        fi
    fi

    if [ "$src_md5" != "$dst_md5" ] ; then
        echo "ERROR: compare_files: md5 of $src_file ($src_md5) doesn't match with md5 of $dst_file ($dst_md5)"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: compare_files: md5 of $src_file ($src_md5) matches with md5 of $dst_file ($dst_md5)"
        fi
    fi

    return 0
}

function compare_dirs()
{
    local src_dir=$1
    local dst_dir=$2
    local verbose=$3

    local src_prefix=$4

    echo "compare dir $src_dir with $dst_dir"

    if [ ! -d $src_dir ] ; then
        echo "ERROR: compare_dirs: param1 $src_dir doesn't exist or isn't a dir"
        return 1
    fi

    if [ ! -d $dst_dir ] ; then
        echo "ERROR: compare_dirs: param2 $dst_dir doesn't exist or isn't a dir"
        return 1
    fi

    local listParam=""
    if [ "X$src_prefix" == "XNoPrefix" ] ; then
        listParam=$src_dir
    else
        listParam=$src_dir/${src_prefix}*
    fi

    local item=""
    for item in `ls -a $listParam` ; do 
        if [[ "$item" == "." ]] || [[ "$item" == ".." ]] ; then
            continue
        fi

        if [ -f $src_dir/$item ] ; then
            echo "file item=$item"
            compare_files $src_dir/$item $dst_dir/$item
            if [ $? -ne 0 ] ; then
                echo "ERROR: compare_dirs: files $src_dir/$item and $dst_dir/$item don't match with eath other"
                return 1
            else
                if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
                    echo "INFO: compare_dirs: files $src_dir/$item and $dst_dir/$item match with eath other"
                fi
            fi
        elif [ -d $src_dir/$item ] ; then
            echo "dir item=$item"
            compare_dirs $src_dir/$item $dst_dir/$item $verbose
            if [ $? -ne 0 ] ; then
                echo "ERROR: compare_dirs: subdirs $src_dir/$item and $dst_dir/$item don't match with eath other"
                return 1
            else
                if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
                    echo "INFO: compare_dirs: subdirs $src_dir/$item and $dst_dir/$item match with eath other"
                fi
            fi
        fi
    done

    return 0
}

function compare_dirs_bilaterally()
{
    local dir1=$1
    local dir2=$2
    local verbose=$3

    local dir1_prefix=$4
    local dir2_prefix=$5

    [ -z "$dir1_prefix" ] && dir1_prefix="NoPrefix"
    [ -z "$dir2_prefix" ] && dir2_prefix="NoPrefix"

    if [ -z "$dir1" -o -z "$dir2" -o -z "$verbose" ] ; then
        echo "ERROR: Invalid Argument: dir1, dir2 and verbose must be present"
        return 1
    fi

    compare_dirs $dir1 $dir2 $verbose $dir1_prefix
    if [ $? -ne 0 ] ; then
        echo "ERROR: compare_dirs_bilaterally: $dir1 doesn't match with $dir2"
        return 1
    fi

    compare_dirs $dir2 $dir1 $verbose $dir2_prefix
    if [ $? -ne 0 ] ; then
        echo "ERROR: compare_dirs_bilaterally: $dir2 doesn't match with $dir1"
        return 1
    fi

    return 0
}

function create_bucket()
{
    local buck_name=$1
    local user_acc_id=$2
    local user_sec_key=$3

    ./put_buck.sh -b $buck_name -i $user_acc_id -k $user_sec_key 2>&1 | grep "HTTP/1.1 200 OK"
    if [ $? -ne 0 ] ; then
        echo "ERROR: create_bucket: failed to create bucket $buck_name"
        return 1
    fi

    return 0
}

function delete_bucket()
{
    local buck_name=$1
    local user_acc_id=$2
    local user_sec_key=$3

    ./del_buck.sh -b $buck_name -i $user_acc_id -k $user_sec_key 2>&1 | grep "HTTP/1.1 204 No Content"
    if [ $? -ne 0 ] ; then
        echo "ERROR: delete_bucket: failed to remove bucket $buckname"
        return 1
    fi

    return 0
}

function bucket_test()
{
    local user_acc_id=$1
    local user_sec_key=$2
    local verbose=$3

    local tmpdir=/tmp/goofys_test_temp_$timestamp
    rm -fr $tmpdir
    mkdir $tmpdir

    for i in {1..1200} ; do
        local buckname=goofys_testbuck_${timestamp}_${i}
        mkdir $tmpdir/$buckname
        create_bucket $buckname $user_acc_id $user_sec_key
        if [ $? -ne 0 ] ; then
            echo "ERROR: create_bucket failed"
            return 1
        fi
    done

    compare_dirs_bilaterally $UsrDir $tmpdir $verbose "goofys_testbuck_${timestamp}_" "NoPrefix"
    if [ $? -ne 0 ] ; then
        echo "ERROR: create_buckets: $UsrDir and $tmpdir don't match with eath other"
        return 1
    fi

    for i in {1..1200} ; do
        local buckname=goofys_testbuck_${timestamp}_${i}
        delete_bucket $buckname $user_acc_id $user_sec_key
        if [ $? -ne 0 ] ; then
            echo "ERROR: delete_bucket failed"
            return 1
        fi
    done

    rm -fr $tmpdir

    return 0
}

function file_ops()
{
    local pathname=$1
    local goofysdir=$2
    local cleanup=$3  # 1: with cleanup;        0: without cleanup
    local verbose=$4  # 1: print verbose info;  0: not print verbose info;

    #write
    cp -f $pathname $goofysdir
    if [ $? -ne 0 ] ; then
        echo "ERROR: file_ops: failed to write file $pathname to $goofysdir"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: file_ops: succeeded to write file $pathname to $goofysdir"
        fi
    fi

    local shortname=`basename $pathname`
    local goofys_pathname=${goofysdir}/${shortname}

    compare_files $pathname $goofys_pathname $verbose
    if [ $? -ne 0 ] ; then
        echo "ERROR: file_ops: $pathname and $goofys_pathname don't match with eath other"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: file_ops: $pathname and $goofys_pathname match with eath other"
        fi
    fi

    #copy
    local goofys_bak=${goofys_pathname}.bak
    cp -f $goofys_pathname $goofys_bak
    if [ $? -ne 0 ] ; then
        echo "ERROR: file_ops: failed to copy $goofys_pathname to $goofys_bak"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: file_ops: succeeded to copy $goofys_pathname to $goofys_bak"
        fi
    fi

    compare_files $goofys_pathname $goofys_bak $verbose
    if [ $? -ne 0 ] ; then
        echo "ERROR: file_ops: $goofys_pathname and $goofys_bak don't match with eath other"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: file_ops: $goofys_pathname and $goofys_bak match with eath other"
        fi
    fi

    #read
    local readfile=/tmp/${shortname}-readfile-`date +%s`
    cp -f $goofys_pathname $readfile
    if [ $? -ne 0 ] ; then
        echo "ERROR: file_ops: failed to copy $goofys_pathname to $readfile"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: file_ops: succeeded to copy $goofys_pathname to $readfile"
        fi
    fi

    compare_files $goofys_pathname $readfile $verbose
    if [ $? -ne 0 ] ; then
        echo "ERROR: file_ops: $goofys_pathname and $readfile don't match with eath other"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: file_ops: $goofys_pathname and $readfile match with eath other"
        fi
        rm -f $readfile
    fi

    #overwrite1
    cp -f $pathname $goofys_pathname
    if [ $? -ne 0 ] ; then
        echo "ERROR: file_ops: failed to overwrite file $goofys_pathname"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: file_ops: succeeded to overwrite $goofys_pathname"
        fi
    fi

    compare_files $pathname $goofys_pathname $verbose
    if [ $? -ne 0 ] ; then
        echo "ERROR: file_ops: $pathname and $goofys_pathname don't match with eath other after overwrite"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: file_ops: $pathname and $goofys_pathname match with eath other after overwrite"
        fi
    fi

    #overwrite2
    cp -f $goofys_pathname $goofys_bak
    if [ $? -ne 0 ] ; then
        echo "ERROR: file_ops: failed to overwrite $goofys_bak"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: file_ops: succeeded to overwrite $goofys_bak"
        fi
    fi

    compare_files $goofys_pathname $goofys_bak $verbose
    if [ $? -ne 0 ] ; then
        echo "ERROR: file_ops: $goofys_pathname and $goofys_bak don't match with eath other after overwrite"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: file_ops: $goofys_pathname and $goofys_bak match with eath other after overwrite"
        fi
    fi


    #cleanup
    if [[ -z "$cleanup" ]] || [[ $cleanup -eq 0 ]] ; then 
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: file_ops: succeeded without cleanup"
        fi
        return 0
    fi

    rm -f $goofys_bak 
    if [ $? -ne 0 ] ; then
        echo "ERROR: file_ops: failed to delete $goofys_bak"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: file_ops: succeeded to delete $goofys_bak"
        fi
    fi

    ls -l $goofys_bak 2>&1 | grep -E "No such file or directory|没有那个文件或目录" > /dev/null
    if [ $? -ne 0 ] ; then
        echo "ERROR: file_ops: $goofys_bak still exists after deletion"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: file_ops: $goofys_bak is gone after deletion"
        fi
    fi
    
    rm -f $goofys_pathname 
    if [ $? -ne 0 ] ; then
        echo "ERROR: file_ops: failed to delete $goofys_pathname"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: file_ops: succeeded to delete $goofys_pathname"
        fi
    fi

    ls -l $goofys_pathname 2>&1 | grep -E "No such file or directory|没有那个文件或目录" > /dev/null
    if [ $? -ne 0 ] ; then
        echo "ERROR: file_ops: $goofys_pathname still exists after deletion"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: file_ops: $goofys_pathname is gone after deletion"
        fi
    fi

    if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
        echo "INFO: file_ops: succeeded with cleanup"
    fi
    return 0
}

function file_test()
{
    local goofysdir=$1
    local cleanup=$2  # 1: with cleanup;        0: without cleanup
    local verbose=$3  # 1: print verbose info;  0: not print verbose info;

    local small_file="./small_file"
    local medium_file="./medium_file"
    local large_file="./large_file"

    local hidden_small_file="./.hidden_small_file"
    local hidden_medium_file="./.hidden_medium_file"
    local hidden_large_file="./.hidden_large_file"

    #prepare temporary files
    dd if=/dev/zero of=$small_file bs=1024 count=1     # 1KB
    dd if=/dev/zero of=$medium_file bs=1024 count=10   # 10KB
    dd if=/dev/zero of=$large_file bs=1024 count=10240 # 10MB

    dd if=/dev/zero of=$hidden_small_file bs=1024 count=2     # 2KB
    dd if=/dev/zero of=$hidden_medium_file bs=1024 count=20   # 20KB
    dd if=/dev/zero of=$hidden_large_file bs=1024 count=6144  # 6MB

    local file=""
    for file in $small_file $medium_file $large_file $hidden_small_file $hidden_medium_file $hidden_large_file ; do
        file_ops $file $goofysdir $cleanup $verbose
        if [ $? -ne 0 ] ; then
            echo "ERROR: file_test: operations of $file in $goofysdir failed"
            return 1
        else
            if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
                echo "INFO: file_test: operations of $file in $goofysdir succeeded"
            fi
        fi
    done

    rm -f $small_file $medium_file $large_file $hidden_small_file $hidden_medium_file $hidden_large_file
    return 0
}

function create_sub_dirs()
{
    local cur_dir=$1

    local prefix=$2
    local width=$3

    local cur_depth=$4
    local total_depth=$5

    local verbose=$6

    if [ $cur_depth -ge $total_depth ] ; then
        return 0
    fi

    local next_depth=`expr $cur_depth + 1`

    local i=0
    while [ $i -lt $width ] ; do
        local subdir=${cur_dir}/${prefix}_${i}
        mkdir $subdir
        if [ $? -ne 0 ] ; then
            echo "ERROR: create_sub_dirs: failed to mkdir $subdir"
            return 1
        else
            if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
                echo "INFO: create_sub_dirs: succeeded to mkdir $subdir"
            fi
        fi

        create_sub_dirs $subdir $prefix $width $next_depth $total_depth $verbose
        i=`expr $i + 1`
    done

    return 0
}

function create_test_tree()
{
    local parentdir=$1
    local topdir=$2

    local prefix=$3
    local depth=$4
    local width=$5
    local hidden=$6   # 1: hidden dir;          0: non-hidden dir;
    local verbose=$7  # 1: print verbose info;  0: not print verbose info;

    if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then
        echo "Enter create_test_tree: parentdir=$parentdir, topdir=$topdir, prefix=$prefix, depth=$depth, width=$width, hidden=$hidden"
    fi

    mkdir -p $parentdir/$topdir
    if [ $? -ne 0 ] ; then
        echo "ERROR: create_test_tree: failed to mkdir $parentdir/$topdir"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then
            echo "INFO: create_test_tree: succeeded to mkdir $parentdir/$topdir"
        fi
    fi

    create_sub_dirs $parentdir/$topdir $prefix $width 0 $depth $verbose
    if [ $? -ne 0 ] ; then
        echo "ERROR: create_test_tree: failed to create dir tree in $parentdir/$topdir, prefix=$prefix, depth=$depth, width=$width"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then
            echo "INFO: create_test_tree: succeeded to create dir tree in $parentdir/$topdir, prefix=$prefix, depth=$depth, width=$width"
        fi
    fi

    local half_width=`expr $width / 2`
    local half_depth=`expr $depth / 2`
    local middle_path=$topdir
    local i=0
    while [ $i -le $half_depth ] ; do
        middle_path=$middle_path/${prefix}_${half_width}
        i=`expr $i + 1`
    done

    #do file test in middle_path 
    file_test $parentdir/$middle_path 0 $verbose  #0 means not to cleanup the files
    if [ $? -ne 0 ] ; then
        echo "ERROR: create_test_tree: failed to do file test in $parentdir/$middle_path"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: create_test_tree: succeeded to do file test in $parentdir/$middle_path"
        fi
    fi

    #mkdir -p
    local mkdir_path=$topdir
    i=0
    while [ $i -lt $depth ] ; do
        if [[ -n "$hidden" ]] && [[ $hidden -eq 1 ]] ; then
            mkdir_path=$mkdir_path/.MKDIRP_$i
        else
            mkdir_path=$mkdir_path/MKDIRP_$i
        fi
        i=`expr $i + 1`
    done

    mkdir -p $parentdir/$mkdir_path
    if [ $? -ne 0 ] ; then
        echo "ERROR: create_test_tree: failed to mkdir -p $parentdir/$mkdir_path"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then
            echo "INFO: create_test_tree: succeeded to mkdir -p $parentdir/$mkdir_path"
        fi
    fi

    #do file test in mkdir_path
    file_test $parentdir/$mkdir_path 0 $verbose  #0 means not to cleanup the files
    if [ $? -ne 0 ] ; then
        echo "ERROR: create_test_tree: failed to do file test in $parentdir/$mkdir_path"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: create_test_tree: succeeded to do file test in $parentdir/$mkdir_path"
        fi
    fi

    return 0
}


function generic_dir_test()
{
    local goofysdir=$1

    local depth=$2
    local width=$3
    local cleanup=$4
    local hidden=$5   # 1: hidden dir;          0: non-hidden dir;
    local verbose=$6  # 1: print verbose info;  0: not print verbose info;

    local tm=`date +%s`

    local prefix=DIR
    local topdir=dir_w${width}_d${depth}_t${tm}
    if [[ -n "$hidden" ]] && [[ $hidden -eq 1 ]] ; then
        topdir=hidden_$topdir
        prefix=.$prefix
    fi

    #1. build dir-tree in goofysdir
    create_test_tree $goofysdir $topdir $prefix $depth $width $hidden $verbose
    if [ $? -ne 0 ] ; then
        echo "ERROR: generic_dir_test: create_test_tree failed. dir=$goofysdir topdir=$topdir prefix=$prefix depth=$depth width=$width hidden=$hidden verbose=$verbose"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: generic_dir_test: create_test_tree succeeded dir=$goofysdir topdir=$topdir prefix=$prefix depth=$depth width=$width hidden=$hidden verbose=$verbose"
        fi
    fi

    #2. build the same dir-tree in /tmp, so that we can compare the dirs
    create_test_tree /tmp $topdir $prefix $depth $width $hidden $verbose
    if [ $? -ne 0 ] ; then
        echo "ERROR: generic_dir_test: create_test_tree failed. dir=/tmp topdir=$topdir prefix=$prefix depth=$depth width=$width hidden=$hidden verbose=$verbose"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: generic_dir_test: create_test_tree succeeded dir=/tmp topdir=$topdir prefix=$prefix depth=$depth width=$width hidden=$hidden verbose=$verbose"
        fi
    fi

    #3. compare
    compare_dirs_bilaterally /tmp/$topdir $goofysdir/$topdir $verbose
    if [ $? -ne 0 ] ; then
        echo "ERROR: generic_dir_test: /tmp/$topdir and  $goofysdir/$topdir don't match with eath other"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: generic_dir_test: /tmp/$topdir and  $goofysdir/$topdir match with eath other"
        fi
    fi

    #4. copy from /tmp to goofysdir, and compare
    cp -fr /tmp/$topdir $goofysdir/${topdir}.bak1
    if [ $? -ne 0 ] ; then
        echo "ERROR: generic_dir_test: failed to copy /tmp/$topdir to $goofysdir/${topdir}.bak1"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: generic_dir_test: succeeded to copy /tmp/$topdir to $goofysdir/${topdir}.bak1"
        fi
    fi

    compare_dirs_bilaterally /tmp/$topdir $goofysdir/${topdir}.bak1 $verbose
    if [ $? -ne 0 ] ; then
        echo "ERROR: generic_dir_test: /tmp/$topdir and $goofysdir/${topdir}.bak1 don't match with eath other after copy"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: generic_dir_test: /tmp/$topdir and $goofysdir/${topdir}.bak1 match with eath other after copy"
        fi
    fi

    #5. copy from goofysdir to goofysdir, and compare
    cp -fr $goofysdir/$topdir $goofysdir/${topdir}.bak2
    if [ $? -ne 0 ] ; then
        echo "ERROR: generic_dir_test: failed to copy $goofysdir/$topdir to $goofysdir/${topdir}.bak2"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: generic_dir_test: succeeded to copy $goofysdir/$topdir to $goofysdir/${topdir}.bak2"
        fi
    fi

    compare_dirs_bilaterally $goofysdir/$topdir $goofysdir/${topdir}.bak2 $verbose
    if [ $? -ne 0 ] ; then
        echo "ERROR: generic_dir_test: $goofysdir/$topdir and $goofysdir/${topdir}.bak2 don't match with eath other after copy"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: generic_dir_test: $goofysdir/$topdir and $goofysdir/${topdir}.bak2 match with eath other after copy"
        fi
    fi

    #6. overwrite and compare
    cp -fr /tmp/$topdir $goofysdir
    if [ $? -ne 0 ] ; then
        echo "ERROR: generic_dir_test: failed to overwrite $goofysdir/$topdir with /tmp/$topdir"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: generic_dir_test: succeeded to overwrite $goofysdir/$topdir with /tmp/$topdir"
        fi
    fi

    compare_dirs_bilaterally /tmp/$topdir $goofysdir/$topdir $verbose
    if [ $? -ne 0 ] ; then
        echo "ERROR: generic_dir_test: /tmp/$topdir and $goofysdir/$topdir don't match with eath other after overwrite"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: generic_dir_test: /tmp/$topdir and $goofysdir/$topdir match with eath other after overwrite"
        fi
    fi

    #7. move non-empty dirs and compare
    #Yuanguo: according to goofys doc: cannot rename non-empty directories
    mv -f $goofysdir/${topdir}.bak2 $goofysdir/${topdir}.bak3 2>&1 | grep -E  "Directory not empty|目录非空"
    if [ $? -ne 0 ] ; then
        echo "ERROR: generic_dir_test: move non-empty dir should fail with \"Directory not empty\", but it didn't"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: generic_dir_test: move non-empty dir failed with \"Directory not empty\" as expected"
        fi
    fi

    compare_dirs_bilaterally $goofysdir/$topdir $goofysdir/${topdir}.bak2 $verbose
    if [ $? -ne 0 ] ; then
        echo "ERROR: generic_dir_test: $goofysdir/$topdir and $goofysdir/${topdir}.bak2 don't match with eath other after move"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: generic_dir_test: $goofysdir/$topdir and $goofysdir/${topdir}.bak2 match with eath other after copy"
        fi
    fi

    #8. move empty dir
    local emptydir=${prefix}_EMPTY_DIR
    mkdir $goofysdir/$topdir/$emptydir
    if [ $? -ne 0 ] ; then
        echo "ERROR: generic_dir_test: failed to create empty dir $goofysdir/$topdir/$emptydir"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: generic_dir_test: succeeded to create empty dir $goofysdir/$topdir/$emptydir"
        fi
    fi

    local emptydir_moved=${emptydir}.moved
    mv -f $goofysdir/$topdir/$emptydir $goofysdir/$topdir/$emptydir_moved
    if [ $? -ne 0 ] ; then
        echo "ERROR: generic_dir_test: failed to move $goofysdir/$topdir/$emptydir to $goofysdir/$topdir/${emptydir}.moved"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: generic_dir_test: succeeded to move $goofysdir/$topdir/$emptydir to $goofysdir/$topdir/${emptydir}.moved"
        fi
    fi

    #9. touch $width number of files
    local touch_file=${prefix}_TOUCH_FILE
    local i=0
    while [ $i -lt $width ] ; do
        touch $goofysdir/$topdir/${touch_file}_${i}
        if [ $? -ne 0 ] ; then
            echo "ERROR: generic_dir_test: failed to touch $goofysdir/$topdir/${prefix}_file_${i}"
            return 1
        else
            if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
                echo "INFO: generic_dir_test: succeeded to touch $goofysdir/$topdir/${prefix}_file_${i}"
            fi
        fi
        i=`expr $i + 1`
    done

    #10. list test
    local list_ret_file=/tmp/list_${topdir}
    ls -l -a $goofysdir/$topdir > $list_ret_file
    if [ $? -ne 0 ] ; then
        echo "ERROR: generic_dir_test: failed to ls $goofysdir/$topdir"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: generic_dir_test: succeeded to ls $goofysdir/$topdir"
        fi
    fi

    grep $emptydir_moved $list_ret_file   #the empty created and moved in step 8
    if [ $? -ne 0 ] ; then  #not found
        echo "ERROR: generic_dir_test: empty dir ${emptydir}.moved is not found in dir $goofysdir/$topdir, check ls-result-file: $list_ret_file"
        return 1
    fi

    i=0
    while [ $i -lt $width ] ; do
        local subdir_dentry=${prefix}_${i}
        grep $subdir_dentry $list_ret_file
        if [ $? -ne 0 ] ; then  #not found
            echo "ERROR: generic_dir_test: dir $subdir_dentry is not found in dir $goofysdir/$topdir, check ls-result-file: $list_ret_file"
            return 1
        fi

        local file_dentry=${touch_file}_${i}  #the files touched in step 9
        grep $file_dentry $list_ret_file
        if [ $? -ne 0 ] ; then  #not found
            echo "ERROR: generic_dir_test: file $file_dentry is not found in dir $goofysdir/$topdir, check ls-result-file: $list_ret_file"
            return 1
        fi

        i=`expr $i + 1`
    done

    local expected=5   # "total line"    "."    ".."     $emptydir_moved     "the MKDIRP dir created by mkdir -p, see create_test_tree"
    expected=`expr $expected + $width`  # $width number of dirs
    expected=`expr $expected + $width`  # $width number of filed touched in step 9
    local actual=`cat $list_ret_file | wc -l`

    if [ $expected -ne $actual ] ; then
        echo "ERROR: generic_dir_test: total dentries should be $expected, but it is $actual actually"
        return 1
    fi

    #11. cleanup
    if [[ -z "$cleanup" ]] || [[ $cleanup -eq 0 ]] ; then
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: generic_dir_test: succeeded without cleanup"
        fi
        return 0
    fi

    local dir=""
    for dir in $list_ret_file /tmp/$topdir $goofysdir/$topdir $goofysdir/${topdir}.bak1 $goofysdir/${topdir}.bak2 ; do
        rm -fr $dir
        if [ $? -ne 0 ] ; then
            echo "ERROR: generic_dir_test: failed to delete $dir"
            return 1
        else
            if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
                echo "INFO: generic_dir_test: succeeded to delete $dir"
            fi
        fi

        ls -l $dir 2>&1 | grep -E "No such file or directory|没有那个文件或目录" > /dev/null
        if [ $? -ne 0 ] ; then
            echo "ERROR: generic_dir_test: $dir still exists after deletion"
            return 1
        else
            if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
                echo "INFO: generic_dir_test: $dir is gone after deletion"
            fi
        fi
    done

    if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
        echo "INFO: generic_dir_test: succeeded with cleanup"
    fi
    return 0
}

function deep_dir_test()
{
    local goofysdir=$1
    local cleanup=$2
    local verbose=$3     # 1: print verbose info;  0: not print verbose info;

    local depth=20
    local width=1


    #non-hidden dirs
    local hidden=0    
    generic_dir_test $goofysdir $depth $width $cleanup $hidden $verbose 
    if [ $? -ne 0 ] ; then
        echo "ERROR: deep_dir_test: non-hidden dirs test failed: $goofysdir $depth $width $cleanup $hidden $verbose"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: deep_dir_test: non-hidden dirs test succeeded"
        fi
    fi

    #hidden dirs
#    hidden=1
#    generic_dir_test $goofysdir $depth $width $cleanup $hidden $verbose 
#    if [ $? -ne 0 ] ; then
#        echo "ERROR: deep_dir_test: hidden dirs test failed"
#        return 1
#    else
#        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
#            echo "INFO: deep_dir_test: hidden dirs test succeeded"
#        fi
#    fi

    return 0
}

function wide_dir_test()
{
    local goofysdir=$1
    local cleanup=$2
    local verbose=$3     # 1: print verbose info;  0: not print verbose info;

    local depth=1
    local width=1600

    #non-hidden dirs
    local hidden=0    
#    generic_dir_test $goofysdir $depth $width $cleanup $hidden $verbose 
#    if [ $? -ne 0 ] ; then
#        echo "ERROR: wide_dir_test: non-hidden test failed"
#        return 1
#    else
#        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
#            echo "INFO: wide_dir_test: non-hidden test succeeded"
#        fi
#    fi

    #hidden dirs
    hidden=1    
    generic_dir_test $goofysdir $depth $width $cleanup $hidden $verbose 
    if [ $? -ne 0 ] ; then
        echo "ERROR: wide_dir_test: hidden dirs test failed"
        return 1
    else
        if [[ -n "$verbose" ]] && [[ $verbose -eq 1 ]] ; then 
            echo "INFO: wide_dir_test: hidden dirs test succeeded"
        fi
    fi

    return 0
}

#0 bucket test
bucket_test $uaccid $useckey 1
[ $? -ne 0 ] && exit 1

testBuck=testBuck_$timestamp

#1. create bucket 
create_bucket $testBuck $uaccid $useckey
[ $? -ne 0 ] && exit 1

#2. copy a small file, medium file, large file
file_test $UsrDir/$testBuck 1 1
[ $? -ne 0 ] && exit 1

#3. deep dir test
deep_dir_test $UsrDir/$testBuck 1 1
[ $? -ne 0 ] && exit 1

#4. wide dir test
wide_dir_test $UsrDir/$testBuck 1 1
[ $? -ne 0 ] && exit 1

#5. remove the bucket created in step 1
delete_bucket $testBuck $uaccid $useckey
[ $? -ne 0 ] && exit 1

echo "Success"
