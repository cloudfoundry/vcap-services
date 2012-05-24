# Copyright (c) 2009-2011 VMware, Inc.

module VCAP
  module Services
    module VBlob
      module Utils

        include VCAP::Services::Base::Warden

        VBLOB_TIMEOUT = 3

        def data_dir
          File.join(base_dir,'vblob_data')
        end

        def data_dir?
          Dir.exists?(data_dir)
        end

      end
    end
  end
end
