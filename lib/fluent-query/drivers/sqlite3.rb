# encoding: utf-8
require "fluent-query/drivers/dbi"
require "fluent-query/drivers/exception"

module FluentQuery
    module Drivers

         ##
         # SQLite3 database driver.
         #
         
         class SQLite3 < FluentQuery::Drivers::DBI

            ##
            # Known tokens index.
            # (internal cache)
            #

            @@__known_tokens = Hash::new do |hash, key| 
                hash[key] = { }
            end

            ##
            # Indicates token is known.
            #

            public
            def known_token?(group, token_name)
                super(group, token_name, @@__known_tokens)
            end


            ##### EXECUTING

            ##
            # Returns the DBI driver name.
            # @return [String] driver name
            #
            
            public
            def driver_name
                "SQLite3"
            end

            ##
            # Opens the connection.
            #
            # It's lazy, so it will open connection before first request through
            # {@link native_connection()} method.
            #

            public
            def open_connection(settings)
                if not settings[:file]
                    raise FluentQuery::Drivers::Exception::new("Database file name is required for connection.")
                end
                
                super(settings)
            end
            
            ##
            # Builds connection string.
            # @return [String] connection string
            #
            
            public
            def connection_string
                if @_nconnection_settings.nil?
                    raise FluentQuery::Drivers::Exception::new('Connection settings hasn\'t been assigned yet.')
                end
                
                # Gets settings                        
                file = @_nconnection_settings[:file]
                
                # Builds connection string and other parameters
                connection_string = "DBI:SQLite3:" << file.to_s
                
                # Returns
                return connection_string
            end

            ##
            # Returns authentification settings.
            # @return [Array] with username and password
            #
            
            public
            def authentification
                @_nconnection_settings.take_values(:username, :password)
            end
            
        end
    end
end

