package responses

type claudeResponsesOutputIndexes struct {
	ByBlock map[int]int
	Blocks  []int
}

func (indexes *claudeResponsesOutputIndexes) add(block int) {
	if indexes.ByBlock == nil {
		indexes.ByBlock = make(map[int]int)
	}
	if _, exists := indexes.ByBlock[block]; exists {
		return
	}
	indexes.ByBlock[block] = len(indexes.Blocks)
	indexes.Blocks = append(indexes.Blocks, block)
}

func (indexes *claudeResponsesOutputIndexes) lastBlock() int {
	if len(indexes.Blocks) == 0 {
		return -1
	}
	return indexes.Blocks[len(indexes.Blocks)-1]
}
