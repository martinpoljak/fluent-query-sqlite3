# encoding: utf-8
require "hash-utils/hash"   # >= 0.15.0

require "fluent-query/drivers/dbi"
require "fluent-query/drivers/exception"

module FluentQuery
    module Drivers

         ##
         # SQLite3 database driver.
         #
         
         class SQLite3 < FluentQuery::Drivers::DBI

            ##
            # Contains relevant methods index for this driver.
            #
                
            RELEVANT = [:select, :insert, :update, :delete, :begin, :commit, :union]

            ##
            # Contains ordering for typicall queries.
            #
            
            ORDERING = {
                :select => [
                    :select, :from, :join, :groupBy, :having, :where, :orderBy, :limit, :offset
                ],
                :insert => [
                    :insert, :values
                ],
                :update => [
                    :update, :set, :where
                ],
                :delete => [
                    :delete, :where
                ],
                :union => [
                    :union
                ]
            }

            ##
            # Contains operators list.
            #
            # Operators are defined as tokens whose multiple parameters in Array
            # are appropriate to join by itself.
            #
            
            OPERATORS = {
                :and => "AND",
                :or => "OR"
            }

            ##
            # Indicates, appropriate token should be present by one real token, but more input tokens.
            #

            AGREGATE = [:where, :orderBy, :select]

            ##
            # Indicates token aliases.
            #

            ALIASES = {
                :leftJoin => :join,
                :rightJoin => :join,
                :fullJoin => :join
            }

            ##
            # Indicates tokens already required.
            #
            
            protected
            @_tokens_required

            ##
            # Known tokens index.
            # (internal cache)
            #

            @@__known_tokens = Hash::new do |hash, key| 
                hash[key] = { }
            end

            ##
            # Initializes driver.
            #

            public
            def initialize(connection)
                super(connection)

                @relevant = self.class::RELEVANT
                @ordering = self.class::ORDERING
                @operators = self.class::OPERATORS
                @aliases = self.class::ALIASES

                self.class::AGREGATE.each do |i| 
                    @agregate[i] = true
                end
                
                @_tokens_required = { }
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
                @_nconnection_settings.get_values(:user, :password)
            end
            
            ##
            # Executes query conditionally.
            #
            # If query isn't suitable for executing, returns it. In otherwise
            # returns result or number of changed rows.
            #

            public
            def execute_conditionally(query, sym, *args, &block)
                case query.type
                    when :insert
                        if (args[0].kind_of? Symbol) and (args[1].kind_of? Hash)
                            result = query.do!
                        end
                    when :begin
                        if args.empty?
                            result = query.execute!
                        end
                    when :commit, :rollback
                        result = query.execute!
                    else
                        result = nil
                end
                
                return result
            end
        end
    end
end

