/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.type;

import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class TypeUtils
{
    private TypeUtils()
    {
    }

    public static int hashPosition(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return 0;
        }
        return type.hash(block, position);
    }

    public static boolean positionEqualsPosition(Type type, Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        boolean leftIsNull = leftBlock.isNull(leftPosition);
        boolean rightIsNull = rightBlock.isNull(rightPosition);
        if (leftIsNull || rightIsNull) {
            return leftIsNull && rightIsNull;
        }
        return type.equalTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    public static List<Type> resolveTypes(List<TypeSignature> typeNames, final TypeManager typeManager)
    {
        return typeNames.stream()
                .map((TypeSignature type) -> checkNotNull(typeManager.getType(type), "Type '%s' not found", type))
                .collect(toImmutableList());
    }

    public static TypeSignature parameterizedTypeName(String base, TypeSignature... argumentNames)
    {
        return new TypeSignature(base, ImmutableList.copyOf(argumentNames), ImmutableList.of());
    }

    public static int getHashPosition(List<? extends Type> hashTypes, Block[] hashBlocks, int position)
    {
        int[] hashChannels = new int[hashBlocks.length];
        for (int i = 0; i < hashBlocks.length; i++) {
            hashChannels[i] = i;
        }
        HashGenerator hashGenerator = new InterpretedHashGenerator(ImmutableList.copyOf(hashTypes), hashChannels);
        Page page = new Page(hashBlocks);
        return hashGenerator.hashPosition(position, page);
    }

    public static Block getHashBlock(List<? extends Type> hashTypes, Block... hashBlocks)
    {
        checkArgument(hashTypes.size() == hashBlocks.length);
        int[] hashChannels = new int[hashBlocks.length];
        for (int i = 0; i < hashBlocks.length; i++) {
            hashChannels[i] = i;
        }
        HashGenerator hashGenerator = new InterpretedHashGenerator(ImmutableList.copyOf(hashTypes), hashChannels);
        int positionCount = hashBlocks[0].getPositionCount();
        BlockBuilder builder = BigintType.BIGINT.createFixedSizeBlockBuilder(positionCount);
        Page page = new Page(hashBlocks);
        for (int i = 0; i < positionCount; i++) {
            BigintType.BIGINT.writeLong(builder, hashGenerator.hashPosition(i, page));
        }
        return builder.build();
    }

    public static Page getHashPage(Page page, List<? extends Type> types, List<Integer> hashChannels)
    {
        Block[] blocks = Arrays.copyOf(page.getBlocks(), page.getChannelCount() + 1);
        ImmutableList.Builder<Type> hashTypes = ImmutableList.builder();
        Block[] hashBlocks = new Block[hashChannels.size()];
        int hashBlockIndex = 0;

        for (int channel : hashChannels) {
            hashTypes.add(types.get(channel));
            hashBlocks[hashBlockIndex++] = blocks[channel];
        }
        blocks[page.getChannelCount()] = getHashBlock(hashTypes.build(), hashBlocks);
        return new Page(blocks);
    }
}
