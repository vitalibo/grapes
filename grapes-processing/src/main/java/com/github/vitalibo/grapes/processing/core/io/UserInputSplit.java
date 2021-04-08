package com.github.vitalibo.grapes.processing.core.io;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@NoArgsConstructor
@AllArgsConstructor
public class UserInputSplit extends InputSplit implements Writable {

    @Getter
    private int id;
    @Getter
    private int[] users;

    @Override
    public long getLength() {
        return users.length;
    }

    @Override
    public String[] getLocations() {
        return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(users.length);
        for (int user : users) {
            out.writeInt(user);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        users = new int[in.readInt()];
        for (int i = 0; i < users.length; i++) {
            users[i] = in.readInt();
        }
    }

}
