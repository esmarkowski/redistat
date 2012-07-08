require "spec_helper"
include Redistat

describe Redistat::Connection do

  before(:each) do
    @redis = Redistat.connect(:port => 8379, :db => 15, :thread_safe => true)
  end

  after(:each) do 
    Redistat::Connection.close(:default, "Distributed")
  end

  it "should return a Redis::Distributed client" do
    default_options = {:db => 15, :host => 'localhost'}
    Redistat.connect [default_options.merge(:port => 8379), default_options.merge(:port => 8380)]
    Redistat.connection.class.should == Redis::Distributed
  end

  it "should accept an array of node configurations" do
    default_options = {:db => 15, :host => 'localhost'}
    Redistat.connect [default_options.merge(:port => 8379, :id => 'larl'), default_options.merge(:port => 8380, :id => 'bang')]
    Redistat.connection.nodes.map(&:client).map(&:id).should == ["larl","bang"] 
  end

  it "should use the first :ref in a node configuration" do
    default_options = {:db => 15, :host => 'localhost'}
    Redistat.connect [default_options.merge(:port => 8379, :id => "larl", :ref => "Distributed"), default_options.merge(:port => 8380, :id => "bang")]

    Redistat::Connection.references.keys.include?("Distributed").should == true
    Redistat::Connection.close( "Distributed" )

    Redistat.connect [default_options.merge(:port => 8379, :id => "larl"), default_options.merge(:port => 8380, :id => "bang", :ref => "Distributed")]
    Redistat::Connection.references.keys.include?("Distributed").should == true

  end

  it "should accept a custom ref as an array element to connect" do
    default_options = {:db => 15, :host => 'localhost'}
    Redistat.connect [default_options.merge(:port => 8379, :id => "larl"), default_options.merge(:port => 8380, :id => "bang"), :ref => "Distributed"]

    Redistat::Connection.references.keys.include?("Distributed").should == true
  end

  it "should close a referenced connection" do
    default_options = {:db => 15, :host => 'localhost'}
    Redistat.connect [default_options.merge(:port => 8379, :id => "larl"), default_options.merge(:port => 8380, :id => "bang"), :ref => "Distributed"]

    Redistat::Connection.connections.keys.include?("larl_bang").should == true
    Redistat::Connection.references.keys.include?("Distributed").should == true

    Redistat::Connection.close( "Distributed" )

    Redistat::Connection.connections.keys.include?("larl_bang").should == false
    Redistat::Connection.references.keys.include?("Distributed").should == false
  end

end
