local _M =
{
    local_ceph =  "ceph-1",

    cephlist =
    {
        ["ceph-1"] =
        {
            state = 'OK',
            weight = 100,
            rgwlist =
            {
                {"127.0.0.1", "8000"},
            },
        },
    },
}

return _M
