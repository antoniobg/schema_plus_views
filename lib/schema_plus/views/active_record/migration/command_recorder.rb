module SchemaPlus::Views
  module ActiveRecord
    module Migration
      module CommandRecorder
        def create_view(*args, &block)
          record(:create_view, args, &block)
        end

        def drop_view(*args, &block)
          record(:drop_view, args, &block)
        end

        def invert_create_view(args)
          [ :drop_view, [args.first] ]
        end

        def create_materialized_view(*args, &block)
          record(:create_materialized_view, args, &block)
        end

        def drop_materialized_view(*args, &block)
          record(:drop_materialized_view, args, &block)
        end

        def invert_create_materialized_view(args)
          [ :drop_materialized_view, [args.first] ]
        end

      end
    end
  end
end
