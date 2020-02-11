defmodule SharedExamples do
  defmacro shared_examples(do: block) do
    quote do
      defmacro __using__(options) do
        block = unquote(Macro.escape(block))

        quote do
          @moduletag unquote(options)
          unquote(block)
        end
      end
    end
  end
end
