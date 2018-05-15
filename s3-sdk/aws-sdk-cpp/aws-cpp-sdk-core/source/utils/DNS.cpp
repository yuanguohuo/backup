/*
* Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
*  http://aws.amazon.com/apache2.0
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/


//Yuanguo: 
//   with gcc-g++ 4.8.5, aws-cpp-sdk-core-tests fails at run-time:
//         terminate called after throwing an instance of 'std::regex_error'
//         what():  regex_error
//         Aborted (core dumped)
//   The reason is that 4.8.5 is too low to support the regular expression; And 
//   it's known that the regular expression is supported on version 4.9 or above, 
//   so we use regular expression to implement the validation only when version 
//   >= 4.9; for lower versions, we do it by checking the condtions one by one;

#include <aws/core/utils/DNS.h>

#if(defined __GNUC__) and (defined __GNUC_MINOR__) and (__GNUC__*10+__GNUC_MINOR__ >= 49)
#include <regex>
#endif

namespace Aws
{
    namespace Utils
    {
        bool IsValidDnsLabel(const Aws::String& label)
        {
            // Valid DNS hostnames are composed of valid DNS labels separated by a period.
            // Valid DNS labels are characterized by the following:
            // 1- Only contains alphanumeric characters and/or dashes
            // 2- Cannot start or end with a dash
            // 3- Have a maximum length of 63 characters (the entirety of the domain name should be less than 255 bytes)

            //TODO: consider making this regex static and passing std::regex_constants::optimize flag
#if(defined __GNUC__) and (defined __GNUC_MINOR__) and (__GNUC__*10+__GNUC_MINOR__ >= 49)
            const std::regex dnsLabel("^[[:alnum:]](?:[[:alnum:]-]{0,61}[[:alnum:]])?$");
            return regex_search(label, dnsLabel);
#else
            if(label.size()==0)
              return false;

            if(label.size()>63)
              return false;

            for(unsigned int i=0;i<label.size();++i)
            {
              if(
                 label[i] != '-' &&
                 !(label[i]>='0' && label[i]<='9') &&
                 !(label[i]>='a' && label[i]<='z') &&
                 !(label[i]>='A' && label[i]<='Z')
                 )
                return false;
            }

            if(label[0]=='-')
              return false;

            if(label[label.size()-1]=='-')
              return false;


            return true;
#endif
        }
    }
}
