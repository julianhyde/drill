/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from BitControl.Proto

package org.apache.drill.exec.proto;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.dyuproject.protostuff.GraphIOUtil;
import com.dyuproject.protostuff.Input;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Output;
import com.dyuproject.protostuff.Schema;

public final class FragmentStatus implements Externalizable, Message<FragmentStatus>, Schema<FragmentStatus>
{

    public static Schema<FragmentStatus> getSchema()
    {
        return DEFAULT_INSTANCE;
    }

    public static FragmentStatus getDefaultInstance()
    {
        return DEFAULT_INSTANCE;
    }

    static final FragmentStatus DEFAULT_INSTANCE = new FragmentStatus();

    
    private MinorFragmentProfile profile;
    private FragmentHandle handle;

    public FragmentStatus()
    {
        
    }

    // getters and setters

    // profile

    public MinorFragmentProfile getProfile()
    {
        return profile;
    }

    public void setProfile(MinorFragmentProfile profile)
    {
        this.profile = profile;
    }

    // handle

    public FragmentHandle getHandle()
    {
        return handle;
    }

    public void setHandle(FragmentHandle handle)
    {
        this.handle = handle;
    }

    // java serialization

    public void readExternal(ObjectInput in) throws IOException
    {
        GraphIOUtil.mergeDelimitedFrom(in, this, this);
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        GraphIOUtil.writeDelimitedTo(out, this, this);
    }

    // message method

    public Schema<FragmentStatus> cachedSchema()
    {
        return DEFAULT_INSTANCE;
    }

    // schema methods

    public FragmentStatus newMessage()
    {
        return new FragmentStatus();
    }

    public Class<FragmentStatus> typeClass()
    {
        return FragmentStatus.class;
    }

    public String messageName()
    {
        return FragmentStatus.class.getSimpleName();
    }

    public String messageFullName()
    {
        return FragmentStatus.class.getName();
    }

    public boolean isInitialized(FragmentStatus message)
    {
        return true;
    }

    public void mergeFrom(Input input, FragmentStatus message) throws IOException
    {
        for(int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
        {
            switch(number)
            {
                case 0:
                    return;
                case 1:
                    message.profile = input.mergeObject(message.profile, MinorFragmentProfile.getSchema());
                    break;

                case 2:
                    message.handle = input.mergeObject(message.handle, FragmentHandle.getSchema());
                    break;

                default:
                    input.handleUnknownField(number, this);
            }   
        }
    }


    public void writeTo(Output output, FragmentStatus message) throws IOException
    {
        if(message.profile != null)
             output.writeObject(1, message.profile, MinorFragmentProfile.getSchema(), false);


        if(message.handle != null)
             output.writeObject(2, message.handle, FragmentHandle.getSchema(), false);

    }

    public String getFieldName(int number)
    {
        switch(number)
        {
            case 1: return "profile";
            case 2: return "handle";
            default: return null;
        }
    }

    public int getFieldNumber(String name)
    {
        final Integer number = __fieldMap.get(name);
        return number == null ? 0 : number.intValue();
    }

    private static final java.util.HashMap<String,Integer> __fieldMap = new java.util.HashMap<String,Integer>();
    static
    {
        __fieldMap.put("profile", 1);
        __fieldMap.put("handle", 2);
    }
    
}
