# Copyright (c) 2009-2011 VMware, Inc.

class VCAP::Services::Sqlfire::SqlfireError < VCAP::Services::Base::Error::ServiceError
    SQLFIRE_INSUFFICIENT_NODES_ERROR = [31001, HTTP_INTERNAL, 'Needed %s nodes, but only found %s for plan %s']
end
