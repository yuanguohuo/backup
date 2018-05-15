#!/bin/bash

dir=`dirname $0`
. $dir/s3cfg

host=127.0.0.1:6081

function init()
{
    local bucket=$1
    local objname=$2

    local date=$(date --utc -R)
    local header="POST\n\ntext/plain\n${date}\n/$bucket/$objname?uploads"
    #StringToSign = HTTP-Verb + "\n" +   --> POST \n
    #    Content-MD5 + "\n" +            --> ""\n
    #    Content-Type + "\n" +           --> "text/plain"\n
    #    Date + "\n" +                   --> ${date}\n
    #    CanonicalizedAmzHeaders +       --> "" 
    #    CanonicalizedResource;          --> /$bucket/$objname?uploads

    local sig=$(echo -en ${header} | openssl sha1 -hmac ${secret} -binary | base64)

    curl -v -H "Date: ${date}" -H "Expect:" -H "Content-Length: 0" -H "Content-Type: text/plain" -H "Authorization: AWS ${token}:${sig}" -L -X POST "http://$host/$bucket/$objname?uploads" -H "Host: $host"
}

function upload() 
{
    local bucket=$1
    local objname=$2
    local uploadId=$3
    local partNumber=$4
    local part=$5

    #local md5=$(md5sum $part)
    #md5=${md5%\  *}
    #md5=$(echo -en $md5|base64)
    #echo $md5.."x"

    local md5=`./md5base64 $part`

    local length=`ls -l $part | tr -s ' ' | cut -d ' ' -f 5`
    local date=$(date --utc -R)
    local header="PUT\n${md5}\ntext/plain\n${date}\n/$bucket/$objname?partNumber=$partNumber&uploadId=$uploadId"
    echo "$header"

    #StringToSign = HTTP-Verb + "\n" +   --> PUT \n
    #    Content-MD5 + "\n" +            --> ${md5} \n
    #    Content-Type + "\n" +           --> "text/plain" \n
    #    Date + "\n" +                   --> ${date} \n
    #    CanonicalizedAmzHeaders +       --> "" 
    #    CanonicalizedResource;          --> /$bucket/$objname?partNumber=$partNumber&uploadId=$uploadId"

    local sig=$(echo -en ${header} | openssl sha1 -hmac ${secret} -binary | base64)

    curl -i -v                                  \
        -H "Host: $host"                        \
        -H "Date: ${date}"                      \
        -H "Expect:"                            \
        -H "Content-Length: $length"            \
        -H "Content-Type: text/plain"           \
        -H "Authorization: AWS ${token}:${sig}" \
        -H "Content-MD5: $md5"                  \
        -L -X PUT "http://$host/$bucket/$objname?partNumber=$partNumber&uploadId=$uploadId" -T $part
}

function list_uploads()
{
    local bucket=$1
    local maxUploads=$2
    local keyMarker=$3
    local uploadIdMarker=$4

    local optional=""
    if [ -n "$maxUploads" ] ; then
        optional="&max-uploads=${maxUploads}${optional}"
    fi
    if [ -n "$keyMarker" ] ; then
        optional="&key-marker=${keyMarker}${optional}"
    fi
    if [ -n "$uploadIdMarker" ] ; then
        optional="&upload-id-marker=${uploadIdMarker}${optional}"
    fi

    local date=$(date --utc -R)
    local header="GET\n\ntext/plain\n${date}\n/${bucket}?uploads"
    local sig=$(echo -en ${header} | openssl sha1 -hmac ${secret} -binary | base64)

    curl -i -v                                  \
        -H "Date: ${date}"                      \
        -H "Host: $host"                        \
        -H "Expect:"                            \
        -H "Content-Type: text/plain"           \
        -H "Authorization: AWS ${token}:${sig}" \
        -L -X GET "http://$host/${bucket}?uploads${optional}"
}

function list_parts()
{
    local bucket=$1
    local objname=$2
    local uploadId=$3
    local numMarker=$4
    local maxParts=$5

    local optional=""
    if [ -n "$numMarker" ] ; then
        optional="&part-number-marker=${numMarker}${optional}"
    fi
    if [ -n "$maxParts" ] ; then
        optional="&max-parts=${maxParts}${optional}"
    fi

    local date=$(date --utc -R)
    local header="GET\n\ntext/plain\n${date}\n/$bucket/$objname?uploadId=${uploadId}"
    local sig=$(echo -en ${header} | openssl sha1 -hmac ${secret} -binary | base64)

    curl -i -v                                  \
        -H "Date: ${date}"                      \
        -H "Host: $host"                        \
        -H "Expect:"                            \
        -H "Content-Type: text/plain"           \
        -H "Authorization: AWS ${token}:${sig}" \
        -L -X GET "http://$host/$bucket/$objname?uploadId=${uploadId}${optional}"
}

function abort_upload() 
{
    local bucket=$1
    local objname=$2
    local uploadId=$3

    local date=$(date --utc -R)
    local header="DELETE\n\ntext/plain\n${date}\n/$bucket/$objname?uploadId=$uploadId"
    echo "$header"

    local sig=$(echo -en ${header} | openssl sha1 -hmac ${secret} -binary | base64)

    curl -i -v                                   \
        -H "Host: $host"                         \
        -H "Date: ${date}"                       \
        -H "Expect:"                             \
        -H "Content-Length: 0"                   \
        -H "Content-Type: text/plain"            \
        -H "Authorization: AWS ${token}:${sig}"  \
        -L -X DELETE "http://$host/$bucket/$objname?uploadId=$uploadId"
}

function up_complete()
{
    local bucket=$1
    local objname=$2
    local uploadId=$3
    shift 3

    local etags=$*

    local xml_complete_start="<CompleteMultipartUpload>"
    local xml_complete_end="</CompleteMultipartUpload>"
    local xml_part_start="<Part>"
    local xml_part_end="</Part>"
    local xml_partnum_start="<PartNumber>"
    local xml_partnum_end="</PartNumber>"
    local xml_etag_start="<ETag>"
    local xml_etag_end="</ETag>"

    local complete_body=${xml_complete_start}

    local c=1
    for etag in $etags ; do
        complete_body=${complete_body}${xml_part_start}
        complete_body=${complete_body}${xml_partnum_start}
        complete_body=${complete_body}$c
        complete_body=${complete_body}${xml_partnum_end}
        complete_body=${complete_body}${xml_etag_start}
        complete_body=${complete_body}\"${etag}\"
        complete_body=${complete_body}${xml_etag_end}
        complete_body=${complete_body}${xml_part_end}	

        c=`expr $c + 1`
    done

    complete_body=${complete_body}${xml_complete_end}
    echo $complete_body

    local length=$(echo -en ${#complete_body})
    local date=$(date --utc -R)

    #local md5=`echo -en $complete_body | md5sum`
    #md5=${md5%\  *}
    #md5=$(echo -en $md5|base64)

    local md5=`./md5base64 $complete_body`

    local header="POST\n$md5\ntext/plain\n${date}\n/$bucket/$objname?uploadId=$uploadId"
    echo $header
    local sig=$(echo -en ${header} | openssl sha1 -hmac ${secret} -binary | base64)

    curl -i -v                                  \
        -H "Host: $host"                        \
        -H "Date: ${date}"                      \
        -H "Expect:"                            \
        -H "Content-MD5: $md5"                  \
        -H "Content-Length: $length"            \
        -H "Content-Type: text/plain"           \
        -H "Authorization: AWS ${token}:${sig}" \
        -L -X POST "http://$host/$bucket/$objname?uploadId=$uploadId" -d $complete_body
}

case $1 in
    init)
        echo "Initialize multi-upload ..."

        bucket=$2
        objname=$3

        echo "bucket=$bucket objname=$objname"

        if [ -z "$bucket" -o -z "$objname" ] ; then
            echo "Usage: $0 init <bucket> <objname>"
            exit 1
        fi

        init $bucket $objname
        ;;
    upload)
        echo "upload a part ..."

        bucket=$2
        objname=$3
        uploadId=$4
        partNumber=$5
        part=$6

        echo "bucket=$bucket objname=$objname uploadId=$uploadId partNumber=$partNumber part=$part"

        if [ -z "$bucket" -o -z "$objname" -o -z "$uploadId" -o -z "$partNumber" -o -z "$part" ] ; then
            echo "Error: bucket, objname, uploadId, partNumber or part is empty"
            echo "Usage: $0 upload <bucket> <objname> <uploadId> <partNumber> <part>"
            exit 1
        fi

        if [ ! -f $part ] ; then
            echo "Error: $part is not a file. Please use 'split' command to cut a large file into" 
            echo "       several small files, each is a part"
            echo "Usage: $0 upload <bucket> <objname> <uploadId> <partNumber> <part>"
            exit 1
        fi

        upload $bucket $objname $uploadId $partNumber $part
        ;;
    list_parts)
        echo "list parts..."

        bucket=$2
        objname=$3
        uploadId=$4
        numMarker=$5
        maxParts=$6
        shift 6

        if [ -z "$bucket" -o -z "$objname" -o -z "$uploadId" ] ; then
            echo "Error: bucket, objname or uploadId is empty"
            echo "Usage: $0 list_parts <bucket> <objname> <uploadId> [<partNumberMarker>] [<maxParts>]"
            exit 1
        fi

        list_parts $bucket $objname $uploadId $numMarker $maxParts
        ;;
    list_uploads)
        echo "list uploads ..."

        bucket=$2
        maxUploads=$3
        keyMarker=$4
        uploadIdMarker=$5
        shift 5

        if [ -z "$bucket" ] ; then
            echo "Error: bucket is empty"
            echo "Usage: $0 list_uploads <bucket> [<maxUploads>] [<keyMarker>] [<uploadIdMarker>]"
            exit 1
        fi

        list_uploads $bucket $maxUploads $keyMarker $uploadIdMarker
        ;;
    complete)
        echo "Complete multi-upload..."
        
        bucket=$2
        objname=$3
        uploadId=$4
        shift 4

        etags=$*

        if [ -z "$bucket" -o -z "$objname" -o -z "$uploadId" ] ; then
            echo "Error: bucket, objname or uploadId is empty"
            echo "Usage: $0 complete <bucket> <objname> <uploadId> etag1 etag2 etag3 ..."
            exit 1
        fi

        up_complete $bucket $objname $uploadId $etags
        ;;
    abort)
        echo "Complete multi-upload..."
        
        bucket=$2
        objname=$3
        uploadId=$4
        shift 4
        
        abort_upload $bucket $objname $uploadId
        ;;
    *)
        echo "Usage: "
        echo "     $0 list_uploads <bucket> [<maxUploads>] [<keyMarker>] [<uploadIdMarker>]"
        echo "     $0 init <bucket> <object-name>"
        echo "     $0 upload <bucket> <objname> <uploadId> <partNumber> <part>"
        echo "     $0 list_parts <bucket> <objname> <uploadId> [<partNumberMarker>] [<maxParts>]"
        echo "     $0 abort <bucket> <objname> <uploadId>"
        echo "     $0 complete <bucket> <objname> <uploadId> etag1 etag2 etag3 ..."
        exit 1
        ;;
esac

exit
