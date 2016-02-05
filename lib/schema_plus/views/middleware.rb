module SchemaPlus::Views
  module Middleware

    module Dumper
      module Tables

        # Dump views
        def after(env)
          re_view_referent = %r{(?:(?i)FROM|JOIN) \S*\b(\S+)\b}
          env.connection.views.each do |view_name|
            next if env.dumper.ignored?(view_name)
            view = View.new(name: view_name, definition: env.connection.view_definition(view_name))
            env.dump.tables[view.name] = view
            env.dump.depends(view.name, view.definition.scan(re_view_referent).flatten)
          end
          env.connection.matviews.each do |matview_name|
            next if env.dumper.ignored?(matview_name)
            matview = MaterializedView.new(name: matview_name, definition: env.connection.materialized_view_definition(matview_name))
            env.dump.tables[matview.name] = matview
            env.dump.depends(matview.name, matview.definition.scan(re_view_referent).flatten)
          end
        end

        # quacks like a SchemaMonkey Dump::Table
        class View < KeyStruct[:name, :definition]
          def assemble(stream)
            heredelim = "END_VIEW_#{name.upcase}"
            stream.puts <<-ENDVIEW
  create_view "#{name}", <<-'#{heredelim}', :force => true
#{definition}
  #{heredelim}

            ENDVIEW
          end

        # quacks like a SchemaMonkey Dump::Table
        class MaterializedView < KeyStruct[:name, :definition]
          def assemble(stream)
            heredelim = "END_VIEW_#{name.upcase}"
            stream.puts <<-ENDVIEW
  create_materialized_view "#{name}", <<-'#{heredelim}'
#{definition}
  #{heredelim}

            ENDVIEW
          end
        end
      end
    end

    module Schema
      module Tables

        module Mysql
          def after(env)
            Tables.filter_out_views(env)
          end
        end

        module Sqlite3
          def after(env)
            Tables.filter_out_views(env)
          end
        end

        def self.filter_out_views(env)
          env.tables -= env.connection.views(env.query_name)
        end
      end
    end

    #
    # Define new middleware stacks patterned on SchemaPlus::Core's naming
    # for tables

    module Schema
      module Views
        ENV = [:connection, :query_name, :views]
      end
      module ViewDefinition
        ENV = [:connection, :view_name, :query_name, :definition]
      end
      module MaterializedViews
        ENV = [:connection, :query_name, :matviews]
      end
      module MaterializedViewDefinition
        ENV = [:connection, :matview_name, :query_name, :definition]
      end
    end

    module Migration
      module CreateView
        ENV = [:connection, :view_name, :definition, :options]
      end
      module DropView
        ENV = [:connection, :view_name, :options]
      end
      module CreateMaterializedView
        ENV = [:connection, :matview_name, :definition]
      end
      module DropMaterializedView
        ENV = [:connection, :matview_name]
      end
    end
  end

end
