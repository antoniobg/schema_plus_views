module SchemaPlus::Views
  module ActiveRecord
    module ConnectionAdapters
      module PostgresqlAdapter

        def views(name = nil) #:nodoc:
          SchemaMonkey::Middleware::Schema::Views.start(connection: self, query_name: name, views: []) { |env|
            sql = <<-SQL
            SELECT viewname
              FROM pg_views
            WHERE schemaname = ANY (current_schemas(false))
            AND viewname NOT LIKE 'pg\_%'
            SQL
            sql += " AND schemaname != 'postgis'" if adapter_name == 'PostGIS'
            env.views += env.connection.query(sql, env.query_name).map { |row| row[0] }
          }.views
        end

        def materialized_views(name = nil) #:nodoc:
          SchemaMonkey::Middleware::Schema::Views.start(connection: self, query_name: name, views: []) { |env|
            sql = <<-SQL
            SELECT matviewname
              FROM pg_matviews
            WHERE schemaname = ANY (current_schemas(false))
            AND matviewname NOT LIKE 'pg\_%'
            SQL
            sql += " AND schemaname != 'postgis'" if adapter_name == 'PostGIS'
            env.views += env.connection.query(sql, env.query_name).map { |row| row[0] }
          }.views
        end

        def view_definition(view_name, name = nil) #:nodoc:
          SchemaMonkey::Middleware::Schema::ViewDefinition.start(connection: self, view_name: view_name, query_name: name) { |env|
              result = env.connection.query(<<-SQL, name)
                SELECT pg_get_viewdef(oid)
                  FROM pg_class
                WHERE relkind = 'v'
                  AND relname = '#{env.view_name}'
              SQL
              row = result.first
              env.definition = row.first.chomp(';').strip unless row.nil?
          }.definition
        end

        def materialized_view_definition(matview_name, name = nil) #:nodoc:
          SchemaMonkey::Middleware::Schema::ViewDefinition.start(connection: self, matview_name: matview_name, query_name: name) { |env|
              result = env.connection.query(<<-SQL, name)
                SELECT pg_get_viewdef(oid)
                  FROM pg_class
                WHERE relkind = 'm'
                  AND relname = '#{env.matview_name}'
              SQL
              row = result.first
              env.definition = row.first.chomp(';').strip unless row.nil?
          }.definition
        end

      end
    end
  end
end
