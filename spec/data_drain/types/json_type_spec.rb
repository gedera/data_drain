# frozen_string_literal: true

RSpec.describe DataDrain::Types::JsonType do
  let(:type) { described_class.new }

  describe "#cast" do
    it "retorna nil cuando el valor es nil" do
      expect(type.cast(nil)).to be_nil
    end

    it "retorna Hash tal cual" do
      hash = { "a" => 1, "b" => 2 }
      expect(type.cast(hash)).to eq(hash)
    end

    it "retorna Array tal cual" do
      array = [1, 2, 3]
      expect(type.cast(array)).to eq(array)
    end

    it "parsea String JSON a Hash" do
      result = type.cast('{"a":1,"b":2}')
      expect(result).to eq({ "a" => 1, "b" => 2 })
    end

    it "parsea String JSON a Array" do
      result = type.cast("[1,2,3]")
      expect(result).to eq([1, 2, 3])
    end

    it "retorna el valor original si el JSON es invalido" do
      result = type.cast("not json at all")
      expect(result).to eq("not json at all")
    end

    it "no levanta cuando el JSON es invalido" do
      expect { type.cast("broken{json}") }.not_to raise_error
    end

    it "maneja Strings vacios" do
      expect { type.cast("") }.not_to raise_error
    end
  end
end
